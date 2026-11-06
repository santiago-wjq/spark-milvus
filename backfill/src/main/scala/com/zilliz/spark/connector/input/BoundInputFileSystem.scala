package com.zilliz.spark.connector.input

import java.net.URI
import java.util.UUID
import scala.collection.mutable

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{
  BlockLocation,
  FSDataInputStream,
  FSDataOutputStream,
  FileStatus,
  FileSystem,
  FilterFileSystem,
  Path
}
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.util.Progressable
import org.apache.spark.TaskContext

import com.zilliz.milvus.storage.write.WriterResources

/** Hadoop creates these lightweight handles; explicit owners hold delegates. */
final class BoundInputFileSystem extends FilterFileSystem {
  private var boundConfiguration: Configuration = _

  override def initialize(uri: URI, configuration: Configuration): Unit = {
    boundConfiguration = new Configuration(configuration)
    fs = InputFileSystems.acquire(uri, boundConfiguration)
    super.initialize(uri, boundConfiguration)
  }

  override def getConf: Configuration = boundConfiguration
  override def getScheme: String = fs.getScheme

  // Known owners share delegates until task completion or InputSource.close.
  // Remote asynchronous metadata instead owns a delegate per operation/stream.
  override def close(): Unit = ()
}

/** One read, independent of other reads that reuse the same identity value. */
private[input] final class InputReadScope(binding: InputStorageBinding)
    extends AutoCloseable {
  val id: String = UUID.randomUUID().toString
  InputFileSystems.registerDriver(id)

  def optionsOver(inherited: Configuration): Map[String, String] = {
    val resolved = binding.optionsOver(inherited)
    val schemes = Set(
      "file",
      "hdfs",
      "s3a",
      "oss",
      "gs",
      "abfs",
      "abfss",
      "wasb",
      "wasbs"
    ) ++
      resolved.keys.collect {
        case key if key.matches("fs\\.[^.]+\\.impl") => key.split("\\.")(1)
      }
    resolved ++ Map(
      InputFileSystems.ScopeKey -> id,
      InputFileSystems.CreatorJvmKey -> InputFileSystems.creatorJvm
    ) ++ schemes.flatMap { scheme =>
      val key = s"fs.$scheme.impl"
      Seq(
        InputFileSystems.originalKey(scheme) -> resolved.getOrElse(key, ""),
        key -> classOf[BoundInputFileSystem].getName,
        s"fs.$scheme.impl.disable.cache" -> "true"
      )
    }
  }

  override def close(): Unit = InputFileSystems.closeDriver(id)
}

private[input] object InputFileSystems {
  val ScopeKey = "milvus.backfill.input.scope"
  val CreatorJvmKey = "milvus.backfill.input.creator.jvm"
  val creatorJvm: String = UUID.randomUUID().toString
  def originalKey(scheme: String): String =
    s"milvus.backfill.input.original.$scheme.impl"
  private final case class OwnerKey(scope: String, task: Option[Long])
  private val owners = mutable.Map.empty[OwnerKey, Owner]

  private final class Owner {
    private var closed = false
    private val delegates =
      mutable.LinkedHashMap.empty[(String, String), FileSystem]

    def acquire(uri: URI, configuration: Configuration): FileSystem =
      synchronized {
        require(!closed, "Input filesystem scope is closed")
        val scheme = uri.getScheme
        val authority = Option(uri.getAuthority).getOrElse("")
        delegates.getOrElseUpdate(
          (scheme, authority),
          openDelegate(uri, configuration)
        )
      }

    def close(): Unit = {
      val closing = synchronized {
        if (closed) Vector.empty
        else {
          closed = true
          val values = delegates.values.toVector.reverse
          delegates.clear()
          values
        }
      }
      WriterResources.closeAll(
        null,
        closing.map(delegate => () => delegate.close()): _*
      )
    }
  }

  def registerDriver(scope: String): Unit = synchronized {
    owners.put(OwnerKey(scope, None), new Owner)
    ()
  }

  private[input] def openDelegate(
      uri: URI,
      configuration: Configuration
  ): FileSystem = {
    val restored = new Configuration(configuration)
    InputStorageBinding.removeInherited(restored)
    val scheme = uri.getScheme
    val implementation = configuration.get(originalKey(scheme), "")
    if (implementation.isEmpty) restored.unset(s"fs.$scheme.impl")
    else restored.set(s"fs.$scheme.impl", implementation)
    // Hadoop owns factory construction and cleanup if initialization fails.
    FileSystem.newInstance(uri, restored)
  }

  def acquire(uri: URI, configuration: Configuration): FileSystem = {
    val scope = Option(configuration.get(ScopeKey))
      .filter(_.nonEmpty)
      .getOrElse(
        throw new IllegalArgumentException(
          "Input filesystem requires a read scope"
        )
      )
    val context = Option(TaskContext.get())
    val key = OwnerKey(scope, context.map(_.taskAttemptId()))
    val owner = synchronized {
      owners.get(key).orElse {
        context.map { task =>
          val created = new Owner
          task.addTaskCompletionListener[Unit] { _ => closeOwner(key) }
          owners.put(key, created)
          created
        }
      }
    }
    owner match {
      case Some(value) => value.acquire(uri, configuration)
      case None =>
        val creator =
          Option(configuration.get(CreatorJvmKey)).filter(_.nonEmpty)
        require(creator.nonEmpty, "Input filesystem requires a creator JVM")
        if (creator.contains(creatorJvm))
          throw new IllegalStateException(
            "Input filesystem has no open Driver scope or Spark task owner"
          )
        // Spark's footer pool does not inherit TaskContext. Do not guess a task
        // or retain an executor-global owner: operations and streams own leases.
        new OperationInputFileSystem(uri, configuration)
    }
  }

  private def closeOwner(key: OwnerKey): Unit = {
    val owner = synchronized { owners.remove(key) }
    owner.foreach(_.close())
  }

  def closeDriver(scope: String): Unit = closeOwner(OwnerKey(scope, None))
}

/** A resource-free handle for remote metadata threads without TaskContext.
  * Eager operations close their delegate before returning; streams transfer
  * that ownership to their close method. No delegate enters the owner registry.
  */
private[input] final class OperationInputFileSystem(
    uri: URI,
    configuration: Configuration
) extends FileSystem {
  private val boundConfiguration = new Configuration(configuration)
  @volatile private var workingDirectory: Option[Path] = None
  setConf(boundConfiguration)
  super.initialize(uri, boundConfiguration)

  override def getUri: URI =
    new URI(uri.getScheme, uri.getAuthority, "/", null, null)
  override def getScheme: String = uri.getScheme

  private def acquire(): FileSystem = {
    val delegate = InputFileSystems.openDelegate(uri, boundConfiguration)
    try {
      workingDirectory.foreach(delegate.setWorkingDirectory)
      delegate
    } catch {
      case failure: Throwable =>
        WriterResources.closeAll(failure, () => delegate.close())
        throw failure
    }
  }

  private def operation[A](run: FileSystem => A): A = {
    val delegate = acquire()
    WriterResources.closing(delegate.close()) { run(delegate) }
  }

  private def stream[A <: AutoCloseable, B](
      open: FileSystem => A
  )(wrap: (A, () => Unit) => B): B = {
    val delegate = acquire()
    var opened: A = null.asInstanceOf[A]
    try {
      opened = open(delegate)
      var closed = false
      val close = () =>
        synchronized {
          if (!closed) {
            closed = true
            WriterResources.closeAll(
              null,
              () => opened.close(),
              () => delegate.close()
            )
          }
        }
      wrap(opened, close)
    } catch {
      case failure: Throwable =>
        WriterResources.closeAll(
          failure,
          () => if (opened != null) opened.close(),
          () => delegate.close()
        )
        throw failure
    }
  }

  override def open(path: Path, bufferSize: Int): FSDataInputStream =
    stream(_.open(path, bufferSize)) { (opened, release) =>
      new FSDataInputStream(opened) {
        override def close(): Unit = release()
      }
    }

  private def output(
      open: FileSystem => FSDataOutputStream
  ): FSDataOutputStream =
    stream(open) { (opened, release) =>
      new FSDataOutputStream(opened, null, opened.getPos) {
        override def close(): Unit = release()
      }
    }

  override def create(
      path: Path,
      permission: FsPermission,
      overwrite: Boolean,
      bufferSize: Int,
      replication: Short,
      blockSize: Long,
      progress: Progressable
  ): FSDataOutputStream = output(
    _.create(
      path,
      permission,
      overwrite,
      bufferSize,
      replication,
      blockSize,
      progress
    )
  )
  override def append(
      path: Path,
      bufferSize: Int,
      progress: Progressable
  ): FSDataOutputStream =
    output(_.append(path, bufferSize, progress))

  override def getFileStatus(path: Path): FileStatus = operation(
    _.getFileStatus(path)
  )
  override def listStatus(path: Path): Array[FileStatus] = operation(
    _.listStatus(path)
  )
  override def getFileBlockLocations(
      status: FileStatus,
      start: Long,
      length: Long
  ): Array[BlockLocation] =
    operation(_.getFileBlockLocations(status, start, length))
  override def resolvePath(path: Path): Path = operation(_.resolvePath(path))
  override def hasPathCapability(path: Path, capability: String): Boolean =
    operation(_.hasPathCapability(path, capability))
  override def getDefaultBlockSize(path: Path): Long = operation(
    _.getDefaultBlockSize(path)
  )
  override def getDefaultReplication(path: Path): Short = operation(
    _.getDefaultReplication(path)
  )
  override def rename(source: Path, target: Path): Boolean = operation(
    _.rename(source, target)
  )
  override def delete(path: Path, recursive: Boolean): Boolean = operation(
    _.delete(path, recursive)
  )
  override def mkdirs(path: Path, permission: FsPermission): Boolean =
    operation(_.mkdirs(path, permission))
  override def getWorkingDirectory: Path =
    workingDirectory.getOrElse(operation(_.getWorkingDirectory))
  override def setWorkingDirectory(path: Path): Unit = {
    workingDirectory = Some(operation { delegate =>
      delegate.setWorkingDirectory(path)
      delegate.getWorkingDirectory
    })
  }
  override def close(): Unit = ()
}

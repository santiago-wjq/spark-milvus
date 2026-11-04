package com.zilliz.spark.connector.input

import java.io.{DataInputStream, DataOutputStream, IOException}
import java.net.URI
import java.nio.file.Files
import java.util.concurrent.{
  Callable,
  ConcurrentHashMap,
  ForkJoinPool,
  TimeUnit
}
import java.util.concurrent.atomic.AtomicInteger
import java.util.UUID
import scala.jdk.CollectionConverters._
import scala.util.Using

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{
  FSDataInputStream,
  FileStatus,
  FileSystem,
  FilterFileSystem,
  Path,
  RawLocalFileSystem
}
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.spark.sql.SparkSession
import org.apache.spark.TaskContext
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll

/** Tracks actual delegate creation and closure, including Spark task callbacks.
  */
private[input] object CountedInputFileSystems {
  private val opened = new ConcurrentHashMap[String, AtomicInteger]()
  private val closed = new ConcurrentHashMap[String, AtomicInteger]()
  def open(tag: String): Unit = {
    opened.computeIfAbsent(tag, _ => new AtomicInteger()).incrementAndGet(); ()
  }
  def close(tag: String): Unit = {
    closed.computeIfAbsent(tag, _ => new AtomicInteger()).incrementAndGet(); ()
  }
  def opens(tag: String): Int = Option(opened.get(tag)).fold(0)(_.get())
  def closes(tag: String): Int = Option(closed.get(tag)).fold(0)(_.get())
}

/** Hadoop instantiates this public no-argument class by its configured name. */
class CountedInputFileSystem extends RawLocalFileSystem {
  private var tag: String = _
  private var uri: URI = URI.create("counted:///")
  private var closeFails = false
  private var closeInterrupts = false

  override def initialize(name: URI, configuration: Configuration): Unit = {
    tag = configuration.get("fs.counted.test.tag")
    uri = new URI(name.getScheme, name.getAuthority, "/", null, null)
    closeFails = configuration.getBoolean("fs.counted.test.close-fails", false)
    closeInterrupts =
      configuration.getBoolean("fs.counted.test.close-interrupts", false)
    super.initialize(name, configuration)
    CountedInputFileSystems.open(tag)
    if (configuration.getBoolean("fs.counted.test.initialize-fails", false))
      throw new IOException("delegate initialization failed")
  }
  // RawLocalFileSystem calls this while its superclass constructor is running.
  override def getUri: URI = if (uri == null) URI.create("counted:///") else uri
  override def getScheme: String = getUri.getScheme
  override def getFileStatus(path: Path): FileStatus = {
    if (getConf.getBoolean("fs.counted.test.metadata-fails", false))
      throw new IOException("metadata failed")
    super.getFileStatus(path)
  }
  override def open(path: Path, bufferSize: Int): FSDataInputStream = {
    if (getConf.getBoolean("fs.counted.test.open-fails", false))
      throw new IOException("stream open failed")
    val opened = super.open(path, bufferSize)
    new FSDataInputStream(opened) {
      override def close(): Unit = {
        opened.close()
        if (getConf.getBoolean("fs.counted.test.stream-close-fails", false))
          throw new IOException("stream close failed")
      }
    }
  }
  override def close(): Unit = {
    CountedInputFileSystems.close(tag)
    super.close()
    if (closeInterrupts)
      throw new InterruptedException("delegate close interrupted")
    if (closeFails) throw new IOException("delegate close failed")
  }
}

/** Executed in a separate JVM: no local Driver owner can hide missing ownership
  * on Spark-style footer worker threads.
  */
object RemoteInputMetadataProbe {
  def main(args: Array[String]): Unit = {
    val config = new Configuration(false)
    Using.resource(
      new DataInputStream(Files.newInputStream(java.nio.file.Path.of(args(0))))
    ) {
      config.readFields
    }
    require(
      config.get(InputFileSystems.CreatorJvmKey) != InputFileSystems.creatorJvm
    )
    val pool = new ForkJoinPool(2)
    try {
      pool
        .submit(new Callable[Unit] {
          override def call(): Unit = {
            require(TaskContext.get() == null)
            val tag = config.get("fs.counted.test.tag")
            val path = new Path(args(1))
            val fs = path.getFileSystem(config)
            require(CountedInputFileSystems.opens(tag) == 0)
            val status = fs.getFileStatus(path)
            require(status.getLen == 4)
            require(CountedInputFileSystems.opens(tag) == 1)
            require(CountedInputFileSystems.closes(tag) == 1)
            val stream = fs.open(path, 4096)
            require(CountedInputFileSystems.opens(tag) == 2)
            require(CountedInputFileSystems.closes(tag) == 1)
            fs.close()
            stream.seek(2)
            require(stream.read() == 3)
            val bytes = new Array[Byte](4)
            stream.readFully(0, bytes)
            require(bytes.toSeq == Seq[Byte](1, 2, 3, 4))
            stream.close()
            stream.close()
            require(CountedInputFileSystems.closes(tag) == 2)
            require(fs.getFileBlockLocations(status, 0, 4).nonEmpty)
            require(CountedInputFileSystems.opens(tag) == 3)
            require(CountedInputFileSystems.closes(tag) == 3)
            val parquetPath = new Path(args(2))
            val input =
              HadoopInputFile.fromStatus(fs.getFileStatus(parquetPath), config)
            Using.resource(ParquetFileReader.open(input)) { reader =>
              require(
                reader.getFooter.getFileMetaData.getSchema
                  .getFieldName(0) == "id"
              )
              require(
                reader.getFooter.getBlocks.asScala.map(_.getRowCount).sum == 4
              )
            }
            require(CountedInputFileSystems.opens(tag) > 3)
            require(
              CountedInputFileSystems.opens(tag) == CountedInputFileSystems
                .closes(tag)
            )
          }
        })
        .get(30, TimeUnit.SECONDS)
    } finally pool.shutdownNow()
    println("REMOTE_METADATA_LEASES_PASSED")
  }
}

class ManagedInputFileSystemTest extends AnyFunSuite with BeforeAndAfterAll {
  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[2,1]")
      .appName("managed-input-filesystem")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
  }
  override def afterAll(): Unit = {
    try if (spark != null) spark.stop()
    finally super.afterAll()
  }

  private def configuration(tag: String): Configuration = {
    val config = new Configuration(false)
    config.setClass(
      "fs.counted.impl",
      classOf[CountedInputFileSystem],
      classOf[FileSystem]
    )
    config.set("fs.counted.test.tag", tag)
    config
  }
  private def bound(
      scope: InputReadScope,
      inherited: Configuration
  ): Configuration = {
    val config = new Configuration(false)
    scope.optionsOver(inherited).foreach { case (key, value) =>
      config.set(key, value)
    }
    config
  }
  private def delegate(fileSystem: FileSystem): FileSystem =
    fileSystem.asInstanceOf[FilterFileSystem].getRawFileSystem

  private def remote(config: Configuration): Configuration = {
    val scope = new InputReadScope(InputStorageBinding.capture(config))
    try {
      val selected = bound(scope, config)
      selected.set(InputFileSystems.CreatorJvmKey, UUID.randomUUID().toString)
      selected
    } finally scope.close()
  }

  private def withFile[A](run: Path => A): A = {
    val file = Files.createTempFile("input-lease-", ".bin")
    Files.write(file, Array[Byte](1, 2, 3, 4))
    try run(new Path(new URI("counted", "", file.toString, null, null)))
    finally Files.deleteIfExists(file)
  }

  test(
    "a separate JVM footer worker closes metadata and stream leases without a Driver owner"
  ) {
    val tag = UUID.randomUUID().toString
    val config = configuration(tag)
    val scope = new InputReadScope(InputStorageBinding.capture(config))
    val serialized = Files.createTempFile("input-binding-", ".bin")
    val output = Files.createTempFile("input-probe-", ".log")
    val parquetDirectory = Files.createTempDirectory("input-footer-")
    try {
      val parquetOutput = parquetDirectory.resolve("data")
      spark.range(0, 4, 1, 1).write.parquet(parquetOutput.toString)
      val parquetFile = Using.resource(Files.list(parquetOutput)) { files =>
        files.iterator().asScala.find(_.toString.endsWith(".parquet")).get
      }
      val parquetPath =
        new Path(new URI("counted", "", parquetFile.toString, null, null))
      Using.resource(new DataOutputStream(Files.newOutputStream(serialized))) {
        bound(scope, config).write
      }
      withFile { path =>
        val process = new ProcessBuilder(
          java.nio.file.Path
            .of(System.getProperty("java.home"), "bin", "java")
            .toString,
          "-cp",
          System.getProperty("java.class.path"),
          RemoteInputMetadataProbe.getClass.getName.stripSuffix("$"),
          serialized.toString,
          path.toString,
          parquetPath.toString
        ).redirectErrorStream(true).redirectOutput(output.toFile).start()
        try {
          assert(
            process.waitFor(60, TimeUnit.SECONDS),
            "Metadata probe timed out"
          )
          val log = Files.readString(output)
          assert(process.exitValue() == 0, log)
          assert(log.contains("REMOTE_METADATA_LEASES_PASSED"), log)
        } finally if (process.isAlive) process.destroyForcibly()
      }
      assert(CountedInputFileSystems.opens(tag) == 0)
    } finally {
      scope.close()
      Files.deleteIfExists(serialized)
      Files.deleteIfExists(output)
      Using.resource(Files.walk(parquetDirectory)) { paths =>
        paths
          .iterator()
          .asScala
          .toVector
          .sortBy(_.getNameCount)
          .reverse
          .foreach(Files.deleteIfExists)
      }
    }
  }

  for (operation <- Seq("metadata", "open")) {
    test(
      s"remote $operation failure preserves the primary error and closes its delegate"
    ) {
      val tag = UUID.randomUUID().toString
      val config = configuration(tag)
      config.setBoolean(s"fs.counted.test.$operation-fails", true)
      config.setBoolean("fs.counted.test.close-fails", true)
      withFile { path =>
        val fs = path.getFileSystem(remote(config))
        val error = intercept[IOException] {
          if (operation == "metadata") fs.getFileStatus(path)
          else fs.open(path, 4096)
        }
        assert(
          error.getMessage == (if (operation == "metadata") "metadata failed"
                               else "stream open failed")
        )
        assert(
          error.getSuppressed.map(_.getMessage).toSeq == Seq(
            "delegate close failed"
          )
        )
        assert(CountedInputFileSystems.opens(tag) == 1)
        assert(CountedInputFileSystems.closes(tag) == 1)
      }
    }
  }

  test(
    "remote stream close attempts the delegate close once and preserves both failures"
  ) {
    val tag = UUID.randomUUID().toString
    val config = configuration(tag)
    config.setBoolean("fs.counted.test.stream-close-fails", true)
    config.setBoolean("fs.counted.test.close-fails", true)
    withFile { path =>
      val fs = path.getFileSystem(remote(config))
      val stream = fs.open(path, 4096)
      assert(stream.read() == 1)
      val error = intercept[IOException] { stream.close() }
      assert(error.getMessage == "stream close failed")
      assert(
        error.getSuppressed.map(_.getMessage).toSeq == Seq(
          "delegate close failed"
        )
      )
      stream.close()
      fs.close()
      assert(CountedInputFileSystems.opens(tag) == 1)
      assert(CountedInputFileSystems.closes(tag) == 1)
    }
  }

  test("remote metadata success cannot hide a delegate close failure") {
    val tag = UUID.randomUUID().toString
    val config = configuration(tag)
    config.setBoolean("fs.counted.test.close-fails", true)
    withFile { path =>
      val fs = path.getFileSystem(remote(config))
      val error = intercept[IOException] { fs.getFileStatus(path) }
      assert(error.getMessage == "delegate close failed")
      assert(CountedInputFileSystems.opens(tag) == 1)
      assert(CountedInputFileSystems.closes(tag) == 1)
    }
  }

  test(
    "remote streams for the same URI retain independent bindings and close independently"
  ) {
    val left = UUID.randomUUID().toString
    val right = UUID.randomUUID().toString
    withFile { path =>
      val first =
        path.getFileSystem(remote(configuration(left))).open(path, 4096)
      val second =
        path.getFileSystem(remote(configuration(right))).open(path, 4096)
      try {
        first.close()
        assert(CountedInputFileSystems.closes(left) == 1)
        assert(CountedInputFileSystems.closes(right) == 0)
        assert(second.read() == 1)
      } finally { first.close(); second.close() }
      assert(CountedInputFileSystems.opens(left) == 1)
      assert(CountedInputFileSystems.opens(right) == 1)
      assert(CountedInputFileSystems.closes(right) == 1)
    }
  }

  test(
    "repeated driver lookups share a delegate until the input owner closes"
  ) {
    val tag = UUID.randomUUID().toString
    val config = configuration(tag)
    val scope = new InputReadScope(InputStorageBinding.capture(config))
    try {
      val selected = bound(scope, config)
      val first = new Path("counted:///first").getFileSystem(selected)
      val second = new Path("counted:///second").getFileSystem(selected)
      assert(delegate(first) eq delegate(second))
      assert(first.getScheme == delegate(first).getScheme)
      assert(CountedInputFileSystems.opens(tag) == 1)
      first.close()
      second.close()
      assert(CountedInputFileSystems.closes(tag) == 0)
      scope.close()
      assert(CountedInputFileSystems.closes(tag) == 1)
      scope.close()
      assert(CountedInputFileSystems.closes(tag) == 1)
      intercept[IllegalStateException] {
        new Path("counted:///late").getFileSystem(selected)
      }
    } finally scope.close()
  }

  test(
    "closing one read scope cannot close another read of the same filesystem identity"
  ) {
    val tag = UUID.randomUUID().toString
    val config = configuration(tag)
    val identity = InputStorageBinding.capture(config)
    val left = new InputReadScope(identity)
    val right = new InputReadScope(identity)
    try {
      val first = new Path("counted:///left").getFileSystem(bound(left, config))
      val rightConfig = bound(right, config)
      val second = new Path("counted:///right").getFileSystem(rightConfig)
      assert(delegate(first) ne delegate(second))
      left.close()
      assert(CountedInputFileSystems.closes(tag) == 1)
      assert(
        delegate(
          new Path("counted:///again").getFileSystem(rightConfig)
        ) eq delegate(second)
      )
      right.close()
      assert(CountedInputFileSystems.opens(tag) == 2)
      assert(CountedInputFileSystems.closes(tag) == 2)
    } finally { left.close(); right.close() }
  }

  test(
    "failed delegate construction is closed and never retained by the read scope"
  ) {
    val tag = UUID.randomUUID().toString
    val config = configuration(tag)
    config.setBoolean("fs.counted.test.initialize-fails", true)
    val scope = new InputReadScope(InputStorageBinding.capture(config))
    try {
      intercept[IOException] {
        new Path("counted:///failure").getFileSystem(bound(scope, config))
      }
      assert(CountedInputFileSystems.opens(tag) == 1)
      assert(CountedInputFileSystems.closes(tag) == 1)
    } finally scope.close()
    assert(CountedInputFileSystems.closes(tag) == 1)
  }

  test(
    "close failure still closes every owned delegate and repeated close is harmless"
  ) {
    val tag = UUID.randomUUID().toString
    val config = configuration(tag)
    config.setBoolean("fs.counted.test.close-fails", true)
    val scope = new InputReadScope(InputStorageBinding.capture(config))
    val selected = bound(scope, config)
    new Path("counted://first/file").getFileSystem(selected)
    new Path("counted://second/file").getFileSystem(selected)
    val failure = intercept[IOException] { scope.close() }
    assert(failure.getSuppressed.length == 1)
    assert(CountedInputFileSystems.opens(tag) == 2)
    assert(CountedInputFileSystems.closes(tag) == 2)
    scope.close()
    assert(CountedInputFileSystems.closes(tag) == 2)
  }

  test(
    "interruption during close is propagated after every delegate is closed"
  ) {
    val tag = UUID.randomUUID().toString
    val config = configuration(tag)
    config.setBoolean("fs.counted.test.close-interrupts", true)
    val scope = new InputReadScope(InputStorageBinding.capture(config))
    val selected = bound(scope, config)
    new Path("counted://first/file").getFileSystem(selected)
    new Path("counted://second/file").getFileSystem(selected)
    val failure = intercept[InterruptedException] { scope.close() }
    assert(failure.getSuppressed.length == 1)
    assert(CountedInputFileSystems.opens(tag) == 2)
    assert(CountedInputFileSystems.closes(tag) == 2)
    scope.close()
    assert(CountedInputFileSystems.closes(tag) == 2)
  }

  for (failTask <- Seq(false, true)) {
    test(s"Spark task completion closes its delegate when failure=$failTask") {
      val tag = UUID.randomUUID().toString
      val config = configuration(tag)
      val scope = new InputReadScope(InputStorageBinding.capture(config))
      val options = scope.optionsOver(config)
      try {
        def execute(): Array[Int] = spark.sparkContext
          .parallelize(Seq(1), 1)
          .map { value =>
            val taskConfig = new Configuration(false)
            options.foreach { case (key, setting) =>
              taskConfig.set(key, setting)
            }
            val first = new Path("counted:///one").getFileSystem(taskConfig)
            val second = new Path("counted:///two").getFileSystem(taskConfig)
            if (
              !(first.asInstanceOf[FilterFileSystem].getRawFileSystem eq
                second.asInstanceOf[FilterFileSystem].getRawFileSystem)
            )
              throw new IllegalStateException(
                "One task opened multiple delegates"
              )
            first.close()
            if (failTask) throw new IOException("task failure")
            value
          }
          .collect()
        if (failTask) intercept[Exception] { execute() }
        else assert(execute().toSeq == Seq(1))
        assert(CountedInputFileSystems.opens(tag) == 1)
        assert(CountedInputFileSystems.closes(tag) == 1)
      } finally scope.close()
      assert(CountedInputFileSystems.closes(tag) == 1)
    }
  }

  test("a delegate close failure makes a successful Spark task fail") {
    val tag = UUID.randomUUID().toString
    val config = configuration(tag)
    config.setBoolean("fs.counted.test.close-fails", true)
    val scope = new InputReadScope(InputStorageBinding.capture(config))
    val options = scope.optionsOver(config)
    try {
      val failure = intercept[Exception] {
        spark.sparkContext
          .parallelize(Seq(1), 1)
          .map { value =>
            val taskConfig = new Configuration(false)
            options.foreach { case (key, setting) =>
              taskConfig.set(key, setting)
            }
            new Path("counted:///completion-failure").getFileSystem(taskConfig)
            value
          }
          .collect()
      }
      assert(failure.getMessage.contains("delegate close failed"))
      assert(CountedInputFileSystems.opens(tag) == 1)
      assert(CountedInputFileSystems.closes(tag) == 1)
    } finally scope.close()
    assert(CountedInputFileSystems.closes(tag) == 1)
  }
}

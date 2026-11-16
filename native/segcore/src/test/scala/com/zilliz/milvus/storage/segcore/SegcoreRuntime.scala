package com.zilliz.milvus.storage.segcore

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path, Paths}
import scala.jdk.CollectionConverters._

import org.scalatest.Assertions

import com.zilliz.milvus.jni.segcore.SegcoreNative

import milvus.proto.plan.plan.PlanNode
import milvus.proto.segcore.segcore.RetrieveResults

/** The segcore build's libraries in a test JVM, found through
  * `milvus.segcore.lib` (native/segcore/build/build.py installs them) and
  * initialized once per JVM; a native suite is canceled without them.
  */
private[segcore] object SegcoreRuntime {

  val libraryDir: Option[Path] =
    sys.props.get("milvus.segcore.lib").map(Paths.get(_))

  private val library: Option[Path] = libraryDir
    .map(_.resolve("libmilvus-segcore-jni.so"))
    .filter(Files.isRegularFile(_))

  /** The limit a retrieve's output fields are checked against, in bytes. A plan
    * without output fields never reaches it (see `SegcorePlan.retrieve`).
    */
  val ResultLimitBytes: Long = 1L << 30

  /** Whether the segcore build's libraries are present. */
  val available: Boolean = library.nonEmpty

  /** Cancels the calling test without the segcore build's libraries. */
  def assumeAvailable(): Unit =
    Assertions.assume(
      available,
      s"the segcore build's libmilvus-segcore-jni.so is not under ${libraryDir
          .getOrElse("milvus.segcore.lib")}; build it with native/segcore/build/build.py"
    )

  /** The directory segcore owns in this JVM, after the process-wide settings,
    * made once: segcore itself, the cache layer sealed segments load through
    * and the local file system their manifests are read through. Native INFO
    * lines go to stderr and would fill the test output. Needs [[available]].
    */
  lazy val work: Path = {
    System.load(library.get.toString)
    val directory = Files.createTempDirectory("segcore-test")
    SegcoreNative.init(
      Runtime.getRuntime.availableProcessors(),
      directory.resolve("segcore").toString,
      1L << 30
    )
    SegcoreNative.setLogLevel("warn")
    // The fixtures take a few KiB.
    SegcoreNative.configureCache(
      1L << 30,
      1L << 30,
      1L << 30,
      1L << 30,
      1L << 30,
      1L << 30,
      directory.resolve("cache").toString
    )
    SegcoreNative.initLocalFileSystem()
    directory
  }

  /** A retrieve of `plan` on `segment` at `timestamp`, parsed. Every result
    * must be complete: segcore marks a truncated one, and none may be.
    */
  def retrieve(
      collection: Long,
      segment: Long,
      plan: PlanNode,
      timestamp: Long
  ): RetrieveResults = {
    val handle = SegcoreNative.createRetrievePlan(collection, plan.toByteArray)
    try {
      val results = RetrieveResults.parseFrom(
        SegcoreNative.retrieve(segment, handle, timestamp, ResultLimitBytes)
      )
      Assertions.assert(!results.hasMoreResult, "a retrieve was truncated")
      results
    } finally SegcoreNative.deleteRetrievePlan(handle)
  }

  /** The segment offsets of the rows a retrieve admitted. */
  def rows(results: RetrieveResults): Set[Int] =
    results.offset.map(Math.toIntExact).toSet

  /** The shared objects this process maps, from `/proc/self/maps`. */
  def mappedLibraries(): Set[Path] =
    Files
      .readAllLines(Paths.get("/proc/self/maps"), UTF_8)
      .asScala
      .flatMap(_.trim.split("\\s+", 6).drop(5).headOption)
      .filter(path => path.startsWith("/") && path.contains(".so"))
      .map(Paths.get(_))
      .toSet
}

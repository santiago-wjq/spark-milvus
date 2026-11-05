package com.zilliz.spark.connector.backfill

import java.io.IOException
import java.net.URI
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.ConcurrentHashMap

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.RawLocalFileSystem
import org.apache.spark.TaskContext

/** Arms only the real driver-owned input delegate after durable publication. */
private[backfill] object CleanupInputFileSystems {
  final class State {
    val driverOpens = new AtomicInteger()
    val driverCloses = new AtomicInteger()
    val publishedCloses = new AtomicInteger()
    val cleanupFailure = new IOException("owned input delegate close failed")
    @volatile var published = false
    @volatile var failClose = false
  }
  private val states = new ConcurrentHashMap[String, State]()
  def register(tag: String): State = {
    val state = new State
    require(states.putIfAbsent(tag, state) == null)
    state
  }
  def state(tag: String): State = Option(states.get(tag)).getOrElse(
    throw new IllegalStateException(s"Unknown recovery input: $tag")
  )
  def remove(tag: String): Unit = { states.remove(tag); () }
}

/** Hadoop loads this no-argument class for a test-only local input scheme. */
class CleanupInputFileSystem extends RawLocalFileSystem {
  private var uri: URI = URI.create("cleanup:///")
  private var state: CleanupInputFileSystems.State = _
  private var driver = false
  override def initialize(name: URI, configuration: Configuration): Unit = {
    uri = new URI(name.getScheme, name.getAuthority, "/", null, null)
    state = CleanupInputFileSystems.state(name.getAuthority)
    driver = TaskContext.get() == null
    super.initialize(name, configuration)
    if (driver) state.driverOpens.incrementAndGet()
  }
  override def getUri: URI =
    if (uri == null) URI.create("cleanup:///") else uri
  override def getScheme: String = getUri.getScheme
  override def close(): Unit = {
    super.close()
    if (driver) {
      state.driverCloses.incrementAndGet()
      if (state.published) {
        state.publishedCloses.incrementAndGet()
        if (state.failClose) throw state.cleanupFailure
      }
    }
  }
}

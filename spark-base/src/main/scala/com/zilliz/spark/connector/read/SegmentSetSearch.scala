package com.zilliz.spark.connector.read

import java.util.concurrent.Semaphore

import org.apache.arrow.memory.BufferAllocator
import org.apache.spark.internal.Logging
import org.apache.spark.sql.types.StructType
import org.apache.spark.TaskContext

import com.zilliz.milvus.storage.expr.PlanParser
import com.zilliz.milvus.storage.index.{
  CandidateBytes,
  MachineResources,
  QueryMatrix,
  SearchPlan,
  SegmentSearch,
  TopKMerger
}
import com.zilliz.milvus.storage.read.exec.{
  RowExclusions,
  SegmentIndexHandle,
  SegmentVectors
}
import com.zilliz.milvus.storage.read.exec.ReadMetrics
import com.zilliz.milvus.storage.schema.VectorLayout
import com.zilliz.spark.connector.metrics.SearchMetrics
import com.zilliz.spark.connector.options.SearchResources
import com.zilliz.spark.connector.types.ArrowAllocator
import io.milvus.grpc.schema.CollectionSchema

/** One first-stage task: one segment set, every query group, the candidates
  * they produce.
  *
  * The task opens what the Milvus format side offers for each of its segments —
  * the vector batches of an exact scan, or the index handle of an index probe —
  * and the computation in `core.index` searches it. A task that answers more
  * than one query group keeps its segment set in memory and runs the groups on
  * it one after another, so the segments are read once whichever way the query
  * set was delivered (docs/design/architecture/vector-search.html section 2.1);
  * a set larger than the task's budget is read again for every group instead
  * (search-resources.html section 3.3).
  */
private[read] object SegmentSetSearch extends Logging {

  private val chosenBatches =
    new java.util.concurrent.ConcurrentHashMap[(Int, Option[Long]), Long]()

  /** The exact scan's base block for this JVM: the option when the call set it,
    * otherwise from the machine's L3, CPU quota and this executor's slots
    * (docs/design/architecture/search-resources.html section 3.2). Probed and
    * logged once per JVM for each distinct request.
    */
  private[read] def exactScanBatch(spec: Spec): Long =
    chosenBatches.computeIfAbsent(
      (spec.slots, spec.batchMaxBytes),
      _ => {
        val choice = SearchResources.exactScanBatch(
          MachineResources.probe(),
          spec.slots,
          spec.batchMaxBytes
        )
        logInfo(s"exact scan batch: ${choice.reason}")
        choice.bytes
      }
    )

  /** What every task of one search needs and the driver already knows. */
  final case class Spec(
      vectorColumn: String,
      layout: VectorLayout,
      fieldId: Long,
      nullable: Boolean,
      k: Int,
      metric: String,
      mode: String,
      filter: Option[String],
      parameters: Map[String, String],
      allowUnindexed: Boolean,
      vectorsMaxBytes: Long,
      arrowMaxBytes: Long,
      slots: Int,
      batchMaxBytes: Option[Long],
      indexLoadsMax: Int
  ) extends Serializable {
    require(
      indexLoadsMax > 0,
      s"A task set loads at least one index: $indexLoadsMax"
    )
  }

  def run(
      set: Seq[MilvusInputPartition],
      spec: Spec,
      groups: Iterator[SearchQueries.Group],
      groupCount: Int,
      metrics: SearchMetrics
  ): Iterator[(Long, Array[Byte])] = {
    require(set.nonEmpty, "A first-stage task has no segments")
    require(groupCount > 0, s"A task answers $groupCount query groups")
    val segments = set.map(_.task.segmentId)
    val partitions =
      set.map(partition => partition.task.segmentId -> partition).toMap
    val allocator = ArrowAllocator.forSearchTask(
      TaskContext.get().partitionId(),
      spec.arrowMaxBytes
    )
    // Every step goes to the accumulators as it finishes. A task of this
    // stage runs for as long as its vectors take, and Spark carries a running
    // task's accumulators on the executor heartbeat, so a total added at the
    // end is a total nobody can see while it matters.
    def stepped(step: SegmentSearch.Progress): Unit = {
      if (step.nativeCalls != 0)
        metrics.knowhereCalls.add(step.nativeCalls.toLong)
      if (step.nativeNanos != 0L) metrics.knowhereNanos.add(step.nativeNanos)
      if (step.compared != 0L) metrics.comparedPairs.add(step.compared)
      if (step.segments != 0) metrics.segmentSearches.add(step.segments.toLong)
    }
    def open(segmentId: Long): SegmentSearch.Source =
      source(partitions(segmentId), spec, allocator.allocator, metrics)
    // One group over the set, reading every segment as it goes: what a single
    // group always does, and what a set too large to hold does for each group.
    def streamed(
        group: SearchQueries.Group
    ): Seq[(Long, Array[Byte])] = {
      val (merger, counters) = searching(group, spec, allocator.allocator) {
        queries =>
          SegmentSearch.run(
            segments,
            open,
            queries,
            spec.k,
            spec.metric,
            spec.parameters,
            allocator.allocator,
            stepped
          )
      }
      report(metrics, counters, merger.size)
      logInfo(
        s"Search task: segments=${counters.segments}, groups=$groupCount, " +
          s"queries=${group.queries}, candidates=${merger.size}, streamed"
      )
      candidates(merger, group)
    }
    if (groupCount == 1) {
      try streamed(groups.next()).iterator
      finally allocator.close()
    } else {
      val held = holdOrStream(set, spec, segments, open, metrics)
      Option(TaskContext.get()).foreach(
        _.addTaskCompletionListener[Unit] { _ =>
          try held.foreach(_.close())
          finally allocator.close()
        }
      )
      held match {
        case None => groups.flatMap(streamed)
        case Some(kept) =>
          groups.flatMap { group =>
            val (merger, counters) =
              searching(group, spec, allocator.allocator) { queries =>
                kept.search(
                  queries,
                  spec.k,
                  spec.metric,
                  spec.parameters,
                  allocator.allocator,
                  stepped
                )
              }
            report(metrics, counters, merger.size)
            logInfo(
              s"Search task: segments=${counters.segments}, groups=$groupCount, " +
                s"queries=${group.queries}, candidates=${merger.size}, held"
            )
            candidates(merger, group)
          }
      }
    }
  }

  /** The segment set read into memory for the groups to share, or None when it
    * does not fit the task's budget and each group reads it again instead.
    *
    * A set whose segments all have a row count is decided before anything is
    * read. A set with an estimated segment is read until it either fits or goes
    * over; what was read by then is released and read again per group, which is
    * the cost of an unrecorded row count, not a failure
    * (docs/design/architecture/search-resources.html section 3.3).
    */
  private def holdOrStream(
      set: Seq[MilvusInputPartition],
      spec: Spec,
      segments: Seq[Long],
      open: Long => SegmentSearch.Source,
      metrics: SearchMetrics
  ): Option[SegmentSearch.Held] =
    SearchPlan.knownBytes(set.map(_.task), spec.layout) match {
      case Some(bytes) if bytes > spec.vectorsMaxBytes =>
        logInfo(
          s"Segment set streamed: $bytes bytes of vectors over the ${spec.vectorsMaxBytes} " +
            s"a task keeps; every query group reads the ${segments.size} segments again"
        )
        None
      case _ =>
        SegmentSearch.hold(
          segments,
          open,
          spec.vectorsMaxBytes,
          spec.layout
        ) match {
          case Right(held) =>
            read(metrics, held.read)
            Some(held)
          case Left(overflow) =>
            read(metrics, overflow.read)
            None
        }
    }

  private def report(
      metrics: SearchMetrics,
      counters: SegmentSearch.Counters,
      candidates: Int
  ): Unit = {
    metrics.candidates.add(candidates.toLong)
    read(metrics, counters.read)
  }

  private def read(metrics: SearchMetrics, of: ReadMetrics): Unit = {
    metrics.readBytes.add(of.arrowBytes)
    metrics.readNanos.add(of.jniNanos)
  }

  private def searching[A](
      group: SearchQueries.Group,
      spec: Spec,
      allocator: BufferAllocator
  )(search: QueryMatrix => A): A = {
    val queries = QueryMatrix.ofPacked(
      group.vectors,
      group.firstQuery,
      group.queries,
      spec.layout,
      allocator
    )
    try search(queries)
    finally queries.close()
  }

  /** The candidates of one group: one record for each query that found
    * anything, named by query id rather than by position in the group, holding
    * that query's best k packed into bytes.
    *
    * One record per query rather than one per candidate is what keeps this
    * stage's output the "queries × k × CandidateBytes.Width" the design counts
    * on, and what lets the merge stage walk two answers instead of aggregating
    * candidate by candidate (docs/design/architecture/vector-search.html
    * section 2.1). The merger's heaps are released as they are packed, so a
    * task holds the queries it has packed as bytes and the rest as objects,
    * never both for the same query.
    */
  private def candidates(
      merger: TopKMerger,
      group: SearchQueries.Group
  ): Seq[(Long, Array[Byte])] = {
    val packed = Vector.newBuilder[(Long, Array[Byte])]
    var query = 0
    while (query < group.queries) {
      val bytes = merger.takePacked(query)
      if (bytes.length > 0) packed += group.ids(query) -> bytes
      query += 1
    }
    packed.result()
  }

  /** The loads one executor runs at once, by permit count. An index is copied
    * into a BinarySet and then deserialized, so a load peaks at twice the
    * index's bytes while the tasks that finished loading hold one times theirs:
    * with `milvus.search.index.loads.max` the peak is bounded by the loads in
    * flight rather than by the task slots, which is what lets a search use
    * every core without sizing the executor for "every task loading at once"
    * (docs/design/architecture/search-resources.html section 3.4).
    *
    * One semaphore per permit count, held only while the index is read: a task
    * that has loaded searches beside the next task's load.
    */
  private val indexLoadPermits =
    new java.util.concurrent.ConcurrentHashMap[Int, Semaphore]()

  private def loading[A](permits: Int, metrics: SearchMetrics)(
      load: => A
  ): A = {
    val gate = indexLoadPermits.computeIfAbsent(
      permits,
      new java.util.function.Function[Int, Semaphore] {
        override def apply(count: Int): Semaphore = new Semaphore(count, true)
      }
    )
    val waited = System.nanoTime()
    gate.acquire()
    metrics.indexLoadWaitNanos.add(System.nanoTime() - waited)
    try load
    finally gate.release()
  }

  /** [[loading]] for a test: the permits are an executor-wide bound, so a test
    * needs to reach them without a segment to open.
    */
  private[read] def loadingForTest[A](permits: Int, metrics: SearchMetrics)(
      load: => A
  ): A = loading(permits, metrics)(load)

  /** What this segment offers the search: its vectors, or its index. */
  private def source(
      partition: MilvusInputPartition,
      spec: Spec,
      allocator: BufferAllocator,
      metrics: SearchMetrics
  ): SegmentSearch.Source = {
    val task = partition.task
    val binding = ColumnBinding(partition, StructType(Seq.empty))
    val collection = CollectionSchema.parseFrom(task.schemaBytes)
    val exclusions = RowExclusions.of(
      task,
      collection,
      spec.filter.map(PlanParser.parse),
      binding.columnNameFor
    )
    val selected =
      if (spec.mode != "index") None
      else
        SegmentIndexHandle.select(
          task,
          spec.fieldId,
          spec.metric,
          spec.allowUnindexed
        )
    selected match {
      case Some(descriptor) =>
        val started = System.nanoTime()
        val excluded = exclusions.bitmap(
          task,
          binding.arrowSchema,
          descriptor.rowCount,
          allocator,
          read(metrics, _)
        )
        metrics.bitmapNanos.add(System.nanoTime() - started)
        val handle = loading(spec.indexLoadsMax, metrics) {
          SegmentIndexHandle.open(
            task,
            descriptor,
            spec.layout,
            spec.nullable
          )
        }
        metrics.indexBytes.add(handle.bytes)
        metrics.indexLoadNanos.add(handle.loadNanos)
        SegmentSearch.Index(task.segmentId, handle, excluded)
      case None =>
        // The block one brute-force call scans is chosen on this executor,
        // for this machine's caches and this executor's concurrency.
        val batch = exactScanBatch(spec)
        SegmentSearch.Exact(
          task.segmentId,
          SegmentVectors.open(
            task.copy(limits = task.limits.copy(batchMaxBytes = batch)),
            binding.arrowSchema,
            binding.columnNameFor,
            binding.arrowColumnFor(spec.vectorColumn),
            spec.layout,
            exclusions,
            allocator
          )
        )
    }
  }
}

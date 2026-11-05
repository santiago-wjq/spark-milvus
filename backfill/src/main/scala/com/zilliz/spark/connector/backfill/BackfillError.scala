package com.zilliz.spark.connector.backfill

/** Sealed trait representing all possible errors during backfill operations
  */
sealed trait BackfillError {
  def message: String
  def cause: Option[Throwable]
  def stage: String = "compute"
  def recovery: Option[BackfillRecovery] = None
  def cleanupComplete: Option[Boolean] = None
}

/** A failed operation with a stable phase and optional non-submittable receipt.
  */
case class BackfillExecutionError(
    override val stage: String,
    message: String,
    cause: Option[Throwable] = None,
    override val recovery: Option[BackfillRecovery] = None,
    override val cleanupComplete: Option[Boolean] = None
) extends BackfillError

/** Task outcomes collected after some segment commits succeeded. */
private[backfill] case class PartialWriteError(
    completedSegments: Map[Long, SegmentBackfillResult],
    failedSegmentIds: Seq[Long],
    message: String,
    cause: Option[Throwable] = None,
    override val cleanupComplete: Option[Boolean] = None
) extends BackfillError {
  override val stage: String = "write"
}

/** Error connecting to Milvus or retrieving metadata
  */
case class ConnectionError(
    message: String,
    cause: Option[Throwable] = None,
    override val cleanupComplete: Option[Boolean] = None
) extends BackfillError

/** Error validating schema compatibility between original collection and new
  * field data
  */
case class SchemaValidationError(
    message: String,
    cause: Option[Throwable] = None,
    override val cleanupComplete: Option[Boolean] = None
) extends BackfillError

/** Error reading new field data from the provided path
  */
case class DataReadError(
    path: String,
    message: String,
    cause: Option[Throwable] = None,
    override val cleanupComplete: Option[Boolean] = None
) extends BackfillError

/** Error processing a specific segment during backfill
  */
case class SegmentProcessingError(
    segmentId: Long,
    message: String,
    cause: Option[Throwable] = None,
    override val cleanupComplete: Option[Boolean] = None
) extends BackfillError {
  override val stage: String = "write"
}

/** Error writing backfill data for a segment
  */
case class WriteError(
    segmentId: Long,
    outputPath: String,
    message: String,
    cause: Option[Throwable] = None,
    override val cleanupComplete: Option[Boolean] = None
) extends BackfillError {
  override val stage: String = "write"
}

/** Error with Spark configuration or join strategy
  */
case class SparkConfigError(
    message: String,
    cause: Option[Throwable] = None,
    override val cleanupComplete: Option[Boolean] = None
) extends BackfillError

/** General unexpected error during backfill
  */
case class UnexpectedError(
    message: String,
    cause: Option[Throwable] = None,
    override val cleanupComplete: Option[Boolean] = None
) extends BackfillError

object BackfillError {

  private[backfill] def withCause(
      error: BackfillError,
      cause: Throwable
  ): BackfillError = error match {
    case value: ConnectionError        => value.copy(cause = Some(cause))
    case value: SchemaValidationError  => value.copy(cause = Some(cause))
    case value: DataReadError          => value.copy(cause = Some(cause))
    case value: SegmentProcessingError => value.copy(cause = Some(cause))
    case value: WriteError             => value.copy(cause = Some(cause))
    case value: SparkConfigError       => value.copy(cause = Some(cause))
    case value: UnexpectedError        => value.copy(cause = Some(cause))
    case value: BackfillExecutionError => value.copy(cause = Some(cause))
    case value: PartialWriteError      => value.copy(cause = Some(cause))
  }

  private[backfill] def withCleanupStatus(
      error: BackfillError,
      complete: Boolean
  ): BackfillError =
    error match {
      case value: ConnectionError =>
        value.copy(cleanupComplete = Some(complete))
      case value: SchemaValidationError =>
        value.copy(cleanupComplete = Some(complete))
      case value: DataReadError => value.copy(cleanupComplete = Some(complete))
      case value: SegmentProcessingError =>
        value.copy(cleanupComplete = Some(complete))
      case value: WriteError => value.copy(cleanupComplete = Some(complete))
      case value: SparkConfigError =>
        value.copy(cleanupComplete = Some(complete))
      case value: UnexpectedError =>
        value.copy(cleanupComplete = Some(complete))
      case value: BackfillExecutionError =>
        value.copy(cleanupComplete = Some(complete))
      case value: PartialWriteError =>
        value.copy(cleanupComplete = Some(complete))
    }

  /** Helper to create error from exception
    */
  def fromException(e: Throwable): BackfillError = {
    UnexpectedError(
      message = s"Unexpected error: ${e.getMessage}",
      cause = Some(e)
    )
  }

  /** Helper to create connection error from exception
    */
  def connectionError(message: String, e: Throwable): ConnectionError = {
    ConnectionError(message, Some(e))
  }

  /** Helper to create schema validation error
    */
  def schemaError(message: String): SchemaValidationError = {
    SchemaValidationError(message)
  }

  /** Helper to create data read error
    */
  def dataReadError(
      path: String,
      message: String,
      e: Option[Throwable] = None
  ): DataReadError = {
    DataReadError(path, message, e)
  }

  /** Helper to create segment processing error
    */
  def segmentError(
      segmentId: Long,
      message: String,
      e: Throwable
  ): SegmentProcessingError = {
    SegmentProcessingError(segmentId, message, Some(e))
  }

  /** Helper to create write error
    */
  def writeError(
      segmentId: Long,
      path: String,
      message: String,
      e: Throwable
  ): WriteError = {
    WriteError(segmentId, path, message, Some(e))
  }
}

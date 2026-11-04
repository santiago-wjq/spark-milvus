package com.zilliz.spark.connector.input

import scala.jdk.CollectionConverters._

import org.apache.hadoop.conf.Configuration

/** A filesystem identity captured on the Driver for one input. */
final class InputStorageBinding private (
    private[input] val hadoopOptions: Map[String, String]
) {

  /** Spark always starts a scan with its shared Hadoop configuration. Override
    * settings deliberately absent from this binding as well as present ones so
    * a removed bucket default cannot reappear during lazy scan planning.
    */
  private[input] def optionsOver(
      inherited: Configuration
  ): Map[String, String] = {
    val globalS3 = hadoopOptions.toSeq
      .filter { case (key, _) =>
        key.startsWith("fs.s3a.") && !key.startsWith("fs.s3a.bucket.")
      }
      .sortBy { case (key, _) => -key.length }
    val inheritedKeys = inherited
      .iterator()
      .asScala
      .map(_.getKey)
      .filter(InputStorageBinding.isBindingKey)
      .toVector
    val overrides = inheritedKeys.flatMap { key =>
      val replacement = hadoopOptions.get(key).orElse {
        if (key.startsWith("fs.s3a.bucket."))
          globalS3.collectFirst {
            case (global, value)
                if key.endsWith(global.stripPrefix("fs.s3a")) =>
              value
          }
        else None
      }
      replacement.map(key -> _)
    }.toMap
    val removed = inheritedKeys.filterNot(key =>
      overrides.contains(key) || hadoopOptions.contains(key)
    )
    overrides ++ hadoopOptions + (InputStorageBinding.RemovedKeys -> removed.sorted
      .mkString(","))
  }

  def newHadoopConfiguration(): Configuration = {
    val configuration = new Configuration(false)
    hadoopOptions.foreach { case (key, value) => configuration.set(key, value) }
    configuration
  }

  override def toString: String = "InputStorageBinding(<redacted>)"
}

object InputStorageBinding {
  private[input] val RemovedKeys = "milvus.backfill.input.unset.keys"
  private[input] def removeInherited(configuration: Configuration): Unit = {
    configuration.getTrimmedStrings(RemovedKeys).foreach(configuration.unset)
    configuration.unset(RemovedKeys)
  }

  private[input] def isBindingKey(key: String): Boolean =
    key.startsWith("fs.") || key.startsWith("hadoop.security.credential.")

  /** Capture filesystem settings only; service credentials never enter a scan.
    * Hadoop's process-wide filesystem cache is keyed by URI and user rather
    * than credentials, so each binding must open its own filesystem instances.
    */
  def capture(configuration: Configuration): InputStorageBinding = {
    val settings = configuration
      .iterator()
      .asScala
      .map(entry => entry.getKey -> configuration.get(entry.getKey))
      .filter { case (key, _) => isBindingKey(key) }
      .toMap
    val schemes = Set("s3a", "oss", "gs", "abfs", "abfss", "wasb", "wasbs") ++
      settings.keys.collect {
        case key if key.matches("fs\\.[^.]+\\.impl") => key.split("\\.")(1)
      }
    new InputStorageBinding(
      settings ++ schemes.map(scheme =>
        s"fs.$scheme.impl.disable.cache" -> "true"
      )
    )
  }
}

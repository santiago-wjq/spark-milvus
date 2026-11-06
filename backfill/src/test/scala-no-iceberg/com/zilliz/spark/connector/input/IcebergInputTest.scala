package com.zilliz.spark.connector.input

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll

final class IcebergInputTest extends AnyFunSuite with BeforeAndAfterAll {
  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("offline-no-iceberg-input-test")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    try if (spark != null) spark.stop()
    finally super.afterAll()
  }

  test("Iceberg input names the missing runtime instead of reading") {
    val error = intercept[IllegalArgumentException] {
      InputAdapter.read(
        spark,
        "inputs.db.rows",
        InputOptions("iceberg"),
        InputStorageBinding.capture(new Configuration(false))
      )
    }
    assert(error.getMessage.contains("Iceberg publishes no runtime"))
    assert(!IcebergInput.available)
  }
}

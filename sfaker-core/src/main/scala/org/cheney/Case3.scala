package org.cheney

import org.apache.spark.sql.SparkSession
import org.cheney.fake.{FakeSourceCatalog, FakeSourceProps}

object Case3 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]")
      .appName("Case0")
//      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", classOf[FakeSourceCatalog].getName)
//      .config("spark.sql.catalog.spark_catalog", classOf[DeltaCatalog].getName)
      .getOrCreate();
      val df = spark.sql(
        """
          |create table fake (
          | id int,
          | sex boolean
          |)
          |using FakeSource
          |tblproperties (
          |spark.sql.fake.source.rowsTotalSize = 10000000,
          |spark.sql.fake.source.partitions = 1,
          |spark.sql.fake.source.unsafe.row.enable = true,
          |spark.sql.fake.source.unsafe.codegen.enable = true
          |)
          |""".stripMargin)
    spark.sql("select id from fake limit 10").explain(true);
    spark.sql("show tables").show();

//    val df = spark.sql(
//      """
//        |CREATE TABLE IF NOT EXISTS default.people10m (
//        |  id INT,
//        |  firstName STRING,
//        |  middleName STRING,
//        |  lastName STRING,
//        |  gender STRING,
//        |  birthDate TIMESTAMP,
//        |  ssn STRING,
//        |  salary INT
//        |) USING DELTA
//        |""".stripMargin);
//    spark.sql("select * from default.people10m").show();
//    spark.sql(
//      """
//        |select * from default.people10m
//        |""".stripMargin).explain(true);

    spark.close();
  }
}

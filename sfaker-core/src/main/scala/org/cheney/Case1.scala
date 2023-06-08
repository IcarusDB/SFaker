package org.cheney

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.{CodegenMode, FormattedMode}

object Case1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]")
      .appName("Case1")
      .getOrCreate();
//    spark.sparkContext.setLogLevel("DEBUG");

    val data = spark.read.json("src/main/resources/case1.json");
    data.createOrReplaceTempView("case");
    spark.sql("select id, count(1) from case group by id").explain(CodegenMode.name);
  }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.sfaker

import org.apache.spark.sql.SparkSession
import org.sfaker.source.{FakeSourceCatalog, FakeSourceProps}

object Case3 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Case0")
//      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config(
        "spark.sql.catalog.spark_catalog",
        classOf[FakeSourceCatalog].getName
      )
//      .config("spark.sql.catalog.spark_catalog", classOf[DeltaCatalog].getName)
      .getOrCreate();
    val df = spark.sql("""
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

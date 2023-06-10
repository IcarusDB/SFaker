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

package org.cheney

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.CodegenMode
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.cheney.fake.FakeSourceProps

object Case0 {
  def main(args: Array[String]): Unit = {
    val startTime = System.currentTimeMillis();
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Case0")
      .getOrCreate();

//        val schema = new StructType()
//          .add("id", DataTypes.IntegerType)
//          .add("sex", DataTypes.BooleanType)
//          .add("roles", DataTypes.createArrayType(
//            DataTypes.createMapType(DataTypes.StringType, DataTypes.IntegerType)
//          ))
//          .add("conf", DataTypes.createMapType(DataTypes.StringType, DataTypes.IntegerType))
//          .add("info", DataTypes.createStructType(
//            new StructType().add("score", DataTypes.IntegerType)
//              .add("class", DataTypes.StringType)
//              .fields
//          ));
    val schema = new StructType()
      .add("id", DataTypes.IntegerType)
      .add("sex", DataTypes.BooleanType)
      .add(
        "roles",
        DataTypes.createArrayType(
          DataTypes.createMapType(
            DataTypes.StringType,
            DataTypes.createStructType(
              new StructType()
                .add("score", DataTypes.IntegerType)
                .add("class", DataTypes.StringType)
                .fields
            )
          )
        )
      )
      .add(
        "conf",
        DataTypes.createMapType(DataTypes.StringType, DataTypes.IntegerType)
      )
      .add(
        "info",
        DataTypes.createStructType(
          new StructType()
            .add("score", DataTypes.IntegerType)
            .add("class", DataTypes.StringType)
            .fields
        )
      );

    val unsafeRowEnable = true;
    val codegenEnable = false;
    val df = spark.read
      .format("FakeSource")
      .schema(schema)
      .option(FakeSourceProps.CONF_ROWS_TOTAL_SIZE, 100)
      .option(FakeSourceProps.CONF_PARTITIONS, 1)
      .option(FakeSourceProps.CONF_UNSAFE_ROW_ENABLE, unsafeRowEnable)
      .option(FakeSourceProps.CONF_UNSAFE_CODEGEN_ENABLE, codegenEnable)
      .load();
    //    df.createOrReplaceTempView("Fake");
    println(df.queryExecution.analyzed);
    println(df.queryExecution.optimizedPlan);
    println(df.queryExecution.sparkPlan);
//    df.explain(true);
//    println(df.count());
    //    df.explain(CodegenMode.name);
    //    df.select(df.col("conf")).show();
//    df.show();
    val endTime = System.currentTimeMillis();
    println(FakeSourceProps.CONF_UNSAFE_ROW_ENABLE + ": " + unsafeRowEnable);
    println(FakeSourceProps.CONF_UNSAFE_CODEGEN_ENABLE + ": " + codegenEnable);
    println("cost: " + (endTime - startTime));
    spark.close();

  }
}

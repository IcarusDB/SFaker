package org.cheney

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.CodegenMode
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.cheney.fake.FakeSourceProps

object Case0 {
  def main(args: Array[String]): Unit = {
    val startTime = System.currentTimeMillis();
    val spark = SparkSession.builder().master("local[*]")
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
      .add("roles", DataTypes.createArrayType(
        DataTypes.createMapType(DataTypes.StringType, DataTypes.createStructType(
          new StructType().add("score", DataTypes.IntegerType)
            .add("class", DataTypes.StringType)
            .fields
        ))
      ))
      .add("conf", DataTypes.createMapType(DataTypes.StringType, DataTypes.IntegerType))
      .add("info", DataTypes.createStructType(
        new StructType().add("score", DataTypes.IntegerType)
          .add("class", DataTypes.StringType)
          .fields
      ));

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

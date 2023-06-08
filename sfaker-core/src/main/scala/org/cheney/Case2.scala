package org.cheney

import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.cheney.fake.generator.code.FakeCodegen
import org.cheney.fake.generator.unsafe.FakeUnsafeGenerator

object Case2 {
  def main(args: Array[String]): Unit = {
    val schema = new StructType()
      .add("id", DataTypes.IntegerType)
      .add("sex", DataTypes.BooleanType)
      .add("name", DataTypes.StringType)
      .add("roles", DataTypes.createArrayType(DataTypes.StringType))
//      //      .add("roles", DataTypes.createArrayType(DataTypes.createArrayType(DataTypes.IntegerType)))
      .add("conf", DataTypes.createMapType(DataTypes.StringType, DataTypes.IntegerType))
      .add("info", DataTypes.createStructType(
        new StructType().add("score", DataTypes.IntegerType)
          .add("class", DataTypes.StringType)
          .fields
      ))
     ;

    val generator = new FakeUnsafeGenerator(schema, true);


    val row = generator.init().gen();
    println(row.getInt(0));
    println(row.getBoolean(1));
    println(row.getUTF8String(2));
    println(row.getArray(3).getUTF8String(0));
    //    println(row.getStruct(4, 2).getString(1));
    println(row.getMap(4).keyArray().getUTF8String(0).toString);
    println(row.getStruct(5, 2).getString(1))
  }
}

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

package org.sfaker.examples

import org.apache.spark.sql.types.{DataTypes, StructType}
import org.sfaker.generator.unsafe.FakeUnsafeGenerator

object Case2 {
  def main(args: Array[String]): Unit = {
    val schema = new StructType()
      .add("id", DataTypes.IntegerType)
      .add("sex", DataTypes.BooleanType)
      .add("name", DataTypes.StringType)
      .add("roles", DataTypes.createArrayType(DataTypes.StringType))
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

    val generator = new FakeUnsafeGenerator(schema, true);

    val row = generator.init().gen();
    println(row.getInt(0));
    println(row.getBoolean(1));
    println(row.getUTF8String(2));
    println(row.getArray(3).getUTF8String(0));
    println(row.getMap(4).keyArray().getUTF8String(0).toString);
    println(row.getStruct(5, 2).getString(1))
  }
}

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

package org.apache.spark.sql.execution.datasources.v2;

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.types.StructType

class PartitionUnsafeReader(
    fakeReader: PartitionReader[InternalRow],
    schema: StructType
) extends PartitionReader[InternalRow] {
  private val attributes = schema.toAttributes
  private val unsafeProjection =
    GenerateUnsafeProjection.generate(attributes, attributes);
  private val rowConverter = () => unsafeProjection.apply(fakeReader.get())

  override def next(): Boolean = fakeReader.next()

  override def get(): InternalRow = rowConverter()

  override def close(): Unit = fakeReader.close()
}

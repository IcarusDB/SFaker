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
  private val unsafeProjection = GenerateUnsafeProjection.generate(attributes, attributes);
  private val rowConverter = () => unsafeProjection.apply(fakeReader.get())

  override def next(): Boolean = fakeReader.next()

  override def get(): InternalRow = rowConverter()

  override def close(): Unit = fakeReader.close()
}

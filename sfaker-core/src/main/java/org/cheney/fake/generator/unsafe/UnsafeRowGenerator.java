package org.cheney.fake.generator.unsafe;

import org.apache.spark.sql.catalyst.expressions.UnsafeRow;

import javax.activation.UnsupportedDataTypeException;

public interface UnsafeRowGenerator {
    public UnsafeRow gen() throws UnsupportedDataTypeException;
}

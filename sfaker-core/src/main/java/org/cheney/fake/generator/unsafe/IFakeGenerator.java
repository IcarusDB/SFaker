package org.cheney.fake.generator.unsafe;

import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter;

import javax.activation.UnsupportedDataTypeException;

public interface IFakeGenerator {
    public abstract static class Creator {
        public abstract IFakeGenerator create();
    }

    public void constructor(UnsafeRowWriter writer);

    public void init();

    public void gen() throws UnsupportedDataTypeException;
}

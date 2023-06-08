package org.cheney.fake.generator.unsafe.type;

import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeWriter;
import org.apache.spark.sql.types.DataType;

import org.cheney.fake.generator.code.FakeCodegen;
import org.cheney.fake.generator.unsafe.FakeTypeGenerator;

public abstract class FakeUnsafeType implements FakeTypeGenerator, FakeCodegen {

    private final UnsafeWriter writer;
    private final DataType type;

    protected FakeUnsafeType(UnsafeWriter writer, DataType type) {
        this.writer = writer;
        this.type = type;
    }

    public UnsafeWriter writer() {
        return writer;
    }

    public DataType type() {
        return type;
    }
}

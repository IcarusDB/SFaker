package org.cheney.fake.generator.unsafe.type;

import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeWriter;
import org.apache.spark.sql.types.DataType;

import org.cheney.fake.generator.FakeGenerator;

public class FakeUnsafeFloatType extends FakeUnsafeType {

    public FakeUnsafeFloatType(UnsafeWriter writer, DataType type) {
        super(writer, type);
    }

    @Override
    public void init() {}

    @Override
    public void genIntoWriter(int ordinal) {
        float val = FakeGenerator.genFloat();
        writer().write(ordinal, val);
    }

    @Override
    public void constructCode(Context ctx) {}

    @Override
    public void initCode(Context ctx) {}

    @Override
    public void genCode(Context ctx, String ordinalName) {
        String valName = ctx.freshVarName();
        String writerName = ctx.getNameOrCreate(writer());
        ctx.addIntoGen(
                "float " + valName + " = org.cheney.fake.generator.FakeGenerator.genFloat();");
        ctx.addIntoGen(writerName + ".write(" + ordinalName + ", " + valName + ");");
    }
}

package org.cheney.fake.generator.unsafe.type;

import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeWriter;
import org.apache.spark.sql.types.DataType;

import org.cheney.fake.generator.FakeGenerator;

public class FakeUnsafeBooleanType extends FakeUnsafeType {

    public FakeUnsafeBooleanType(UnsafeWriter writer, DataType type) {
        super(writer, type);
    }

    @Override
    public void init() {}

    @Override
    public void genIntoWriter(int ordinal) {
        boolean val = FakeGenerator.genBoolean();
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
                "boolean " + valName + " = org.cheney.fake.generator.FakeGenerator.genBoolean();");
        ctx.addIntoGen(writerName + ".write(" + ordinalName + ", " + valName + ");");
    }
}

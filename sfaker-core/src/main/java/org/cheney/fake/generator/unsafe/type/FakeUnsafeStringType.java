package org.cheney.fake.generator.unsafe.type;

import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeWriter;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.unsafe.types.UTF8String;

import org.cheney.fake.generator.FakeGenerator;

public class FakeUnsafeStringType extends FakeUnsafeType {

    public FakeUnsafeStringType(UnsafeWriter writer, DataType type) {
        super(writer, type);
    }

    @Override
    public void init() {}

    @Override
    public void genIntoWriter(int ordinal) {
        String val = FakeGenerator.genString();
        writer().write(ordinal, UTF8String.fromString(val));
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
                "String " + valName + " = org.cheney.fake.generator.FakeGenerator.genString();");
        ctx.addIntoGen(
                writerName
                        + ".write("
                        + ordinalName
                        + ", "
                        + UTF8String.class.getName()
                        + ".fromString("
                        + valName
                        + "));");
    }
}

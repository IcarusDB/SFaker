package org.cheney.fake.generator.unsafe.type;

import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeArrayWriter;
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeWriter;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;

import javax.activation.UnsupportedDataTypeException;

public class FakeUnsafeArrayType extends FakeUnsafeType {

    private static final int DEFAULT_ELEMENT_SIZE = 4;
    private final int elementSize;
    private final DataType elementType;
    private final FakeUnsafeGenericType fakeElementType;

    private final UnsafeArrayWriter arrayWriter;

    public FakeUnsafeArrayType(UnsafeWriter writer, DataType type) {
        this(writer, type, DEFAULT_ELEMENT_SIZE);
    }

    public FakeUnsafeArrayType(UnsafeWriter writer, DataType type, int elementSize) {
        super(writer, type);
        this.elementSize = elementSize;
        this.elementType = ((ArrayType) type()).elementType();
        if (FakeUnsafeGenericType.isPrimitive(this.elementType)) {
            this.arrayWriter = new UnsafeArrayWriter(writer(), this.elementSize);
        } else {
            this.arrayWriter = new UnsafeArrayWriter(writer(), this.elementSize * 2);
        }
        this.fakeElementType = new FakeUnsafeGenericType(this.arrayWriter, this.elementType);
    }

    @Override
    public void init() throws UnsupportedDataTypeException {
        this.fakeElementType.init();
    }

    @Override
    public void genIntoWriter(int ordinal) throws UnsupportedDataTypeException {
        int prevCursor = writer().cursor();
        arrayWriter.initialize(
                FakeUnsafeGenericType.isPrimitive(this.elementType)
                        ? elementSize
                        : elementSize * 2);

        for (int idx = 0; idx < this.elementSize; idx++) {
            this.fakeElementType.genIntoWriter(idx);
        }

        writer().setOffsetAndSizeFromPreviousCursor(ordinal, prevCursor);
    }

    @Override
    public void constructCode(Context ctx) {
        String writerClassName = UnsafeArrayWriter.class.getName();
        String arrayWriterName = ctx.getNameOrCreate(this.arrayWriter);
        String writerName = ctx.getNameOrCreate(this.writer());
        ctx.addFieldDeclare(writerClassName + " " + arrayWriterName + ";");
        if (FakeUnsafeGenericType.isPrimitive(this.elementType)) {
            ctx.addIntoConstructor(
                    arrayWriterName
                            + " = new "
                            + writerClassName
                            + "("
                            + writerName
                            + ", "
                            + this.elementSize
                            + ");");
        } else {
            ctx.addIntoConstructor(
                    arrayWriterName
                            + " = new "
                            + writerClassName
                            + "("
                            + writerName
                            + ", "
                            + this.elementSize * 2
                            + ");");
        }
        this.fakeElementType.constructCode(ctx);
    }

    @Override
    public void initCode(Context ctx) {
        this.fakeElementType.initCode(ctx);
    }

    @Override
    public void genCode(Context ctx, String ordinalName) {
        String prevCursorName = ctx.freshVarName();
        String writerName = ctx.getNameOrCreate(this.writer());
        String arrayWriterName = ctx.getNameOrCreate(this.arrayWriter);
        ctx.addIntoGen("int " + prevCursorName + " = " + writerName + ".cursor();");
        if (FakeUnsafeGenericType.isPrimitive(this.elementType)) {
            ctx.addIntoGen(arrayWriterName + ".initialize(" + this.elementSize + ");");
        } else {
            ctx.addIntoGen(arrayWriterName + ".initialize(" + this.elementSize * 2 + ");");
        }

        String idxName = "idx_" + ctx.freshVarName();
        ctx.addIntoGen(
                "for (int "
                        + idxName
                        + " = 0; "
                        + idxName
                        + " < "
                        + this.elementSize
                        + "; "
                        + idxName
                        + "++) {");
        this.fakeElementType.genCode(ctx, idxName);
        ctx.addIntoGen("}");
        ctx.addIntoGen(
                writerName
                        + ".setOffsetAndSizeFromPreviousCursor("
                        + ordinalName
                        + ", "
                        + prevCursorName
                        + ");");
    }
}

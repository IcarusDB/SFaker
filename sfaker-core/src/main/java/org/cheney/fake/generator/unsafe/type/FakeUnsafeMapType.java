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

package org.cheney.fake.generator.unsafe.type;

import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeArrayWriter;
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeWriter;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.unsafe.Platform;

import javax.activation.UnsupportedDataTypeException;

public class FakeUnsafeMapType extends FakeUnsafeType {

    private static final int DEFAULT_MAP_SIZE = 4;
    private final int elementSize;
    private final DataType keyType;
    private final DataType valueType;

    private final FakeUnsafeGenericType fakeKeyType;
    private final FakeUnsafeGenericType fakeValueType;

    private final UnsafeArrayWriter keyWriter;
    private final UnsafeArrayWriter valueWriter;

    public FakeUnsafeMapType(UnsafeWriter writer, DataType type) {
        this(writer, type, DEFAULT_MAP_SIZE);
    }

    public FakeUnsafeMapType(UnsafeWriter writer, DataType type, int elementSize) {
        super(writer, type);
        this.elementSize = elementSize;
        this.keyType = ((MapType) type).keyType();
        this.valueType = ((MapType) type).valueType();

        if (FakeUnsafeGenericType.isPrimitive(keyType)) {
            this.keyWriter = new UnsafeArrayWriter(writer, this.elementSize);
        } else {
            this.keyWriter = new UnsafeArrayWriter(writer, this.elementSize * 2);
        }

        if (FakeUnsafeGenericType.isPrimitive(valueType)) {
            this.valueWriter = new UnsafeArrayWriter(writer, this.elementSize);
        } else {
            this.valueWriter = new UnsafeArrayWriter(writer, this.elementSize * 2);
        }

        this.fakeKeyType = new FakeUnsafeGenericType(this.keyWriter, keyType);
        this.fakeValueType = new FakeUnsafeGenericType(this.valueWriter, valueType);
    }

    @Override
    public void init() throws UnsupportedDataTypeException {
        this.fakeKeyType.init();
        this.fakeValueType.init();
    }

    @Override
    public void genIntoWriter(int ordinal) throws UnsupportedDataTypeException {
        int prevCursor = writer().cursor();
        writer().grow(8);
        writer().increaseCursor(8);

        int prevTmpCursor = writer().cursor();
        this.keyWriter.initialize(
                FakeUnsafeGenericType.isPrimitive(keyType)
                        ? this.elementSize
                        : this.elementSize * 2);
        for (int idx = 0; idx < this.elementSize; idx++) {
            this.fakeKeyType.genIntoWriter(idx);
        }

        Platform.putLong(
                writer().getBuffer(), prevTmpCursor - 8, writer().cursor() - prevTmpCursor);

        this.valueWriter.initialize(
                FakeUnsafeGenericType.isPrimitive(valueType)
                        ? this.elementSize
                        : this.elementSize * 2);
        for (int idx = 0; idx < this.elementSize; idx++) {
            this.fakeValueType.genIntoWriter(idx);
        }
        writer().setOffsetAndSizeFromPreviousCursor(ordinal, prevCursor);
    }

    @Override
    public void constructCode(Context ctx) {
        String writerClassName = UnsafeArrayWriter.class.getName();
        String writerName = ctx.getNameOrCreate(this.writer());
        String keyWriterName = ctx.getNameOrCreate(this.keyWriter);
        String valueWriterName = ctx.getNameOrCreate(this.valueWriter);

        ctx.addFieldDeclare(writerClassName + " " + keyWriterName + ";");
        if (FakeUnsafeGenericType.isPrimitive(keyType)) {
            ctx.addIntoConstructor(
                    keyWriterName
                            + " = new "
                            + writerClassName
                            + "("
                            + writerName
                            + ", "
                            + this.elementSize
                            + ");");
        } else {
            ctx.addIntoConstructor(
                    keyWriterName
                            + " = new "
                            + writerClassName
                            + "("
                            + writerName
                            + ", "
                            + this.elementSize * 2
                            + ");");
        }

        ctx.addFieldDeclare(writerClassName + " " + valueWriterName + ";");
        if (FakeUnsafeGenericType.isPrimitive(valueType)) {
            ctx.addIntoConstructor(
                    valueWriterName
                            + " = new "
                            + writerClassName
                            + "("
                            + writerName
                            + ", "
                            + this.elementSize
                            + ");");
        } else {
            ctx.addIntoConstructor(
                    valueWriterName
                            + " = new "
                            + writerClassName
                            + "("
                            + writerName
                            + ", "
                            + this.elementSize * 2
                            + ");");
        }

        this.fakeKeyType.constructCode(ctx);
        this.fakeValueType.constructCode(ctx);
    }

    @Override
    public void initCode(Context ctx) {
        this.fakeKeyType.initCode(ctx);
        this.fakeValueType.initCode(ctx);
    }

    @Override
    public void genCode(Context ctx, String ordinalName) {
        String writerName = ctx.getNameOrCreate(this.writer());
        String keyWriterName = ctx.getNameOrCreate(this.keyWriter);
        String valueWriterName = ctx.getNameOrCreate(this.valueWriter);

        String prevCursorName = ctx.freshVarName();
        String prevTmpCursorName = ctx.freshVarName();

        ctx.addIntoGen("int " + prevCursorName + " = " + writerName + ".cursor();");
        ctx.addIntoGen(writerName + ".grow(8);");
        ctx.addIntoGen(writerName + ".increaseCursor(8);");

        ctx.addIntoGen("int " + prevTmpCursorName + " = " + writerName + ".cursor();");

        if (FakeUnsafeGenericType.isPrimitive(keyType)) {
            ctx.addIntoGen(keyWriterName + ".initialize(" + this.elementSize + ");");
        } else {
            ctx.addIntoGen(keyWriterName + ".initialize(" + (this.elementSize * 2) + ");");
        }

        String kIdxName = "idx_" + ctx.freshVarName();
        ctx.addIntoGen(
                "for (int "
                        + kIdxName
                        + " = 0; "
                        + kIdxName
                        + "  < "
                        + this.elementSize
                        + "; "
                        + kIdxName
                        + " ++) {");
        this.fakeKeyType.genCode(ctx, kIdxName);
        ctx.addIntoGen("}");

        ctx.addIntoGen(
                Platform.class.getName()
                        + ".putLong("
                        + writerName
                        + ".getBuffer(), "
                        + prevTmpCursorName
                        + " - 8, "
                        + writerName
                        + ".cursor() - "
                        + prevTmpCursorName
                        + ");");

        if (FakeUnsafeGenericType.isPrimitive(valueType)) {
            ctx.addIntoGen(valueWriterName + ".initialize(" + this.elementSize + ");");
        } else {
            ctx.addIntoGen(valueWriterName + ".initialize(" + this.elementSize * 2 + ");");
        }

        String vIdxName = "idx_" + ctx.freshVarName();
        ctx.addIntoGen(
                "for (int "
                        + vIdxName
                        + " = 0; "
                        + vIdxName
                        + " < "
                        + this.elementSize
                        + "; "
                        + vIdxName
                        + "++) {");
        this.fakeValueType.genCode(ctx, vIdxName);
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

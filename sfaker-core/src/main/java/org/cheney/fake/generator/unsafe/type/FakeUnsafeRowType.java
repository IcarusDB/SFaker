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

import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter;
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeWriter;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import javax.activation.UnsupportedDataTypeException;

public class FakeUnsafeRowType extends FakeUnsafeType {

    private StructField[] fields;
    private FakeUnsafeType[] fieldTypes;
    private UnsafeRowWriter rowWriter;
    private boolean isInner = false;

    public FakeUnsafeRowType(UnsafeWriter writer, DataType type) {
        super(writer, type);
        this.fields = ((StructType) type).fields();
        this.fieldTypes = new FakeUnsafeType[fields.length];
        this.rowWriter = new UnsafeRowWriter(writer, fields.length);
        for (int idx = 0; idx < this.fields.length; idx++) {
            StructField field = fields[idx];
            DataType fieldType = field.dataType();
            fieldTypes[idx] = new FakeUnsafeGenericType(rowWriter, fieldType);
        }
        this.isInner = true;
    }

    public FakeUnsafeRowType(UnsafeRowWriter rowWriter, StructType schema) {
        super(rowWriter, schema);
        this.fields = ((StructType) schema).fields();
        this.fieldTypes = new FakeUnsafeType[fields.length];
        this.rowWriter = rowWriter;
        for (int idx = 0; idx < this.fields.length; idx++) {
            StructField field = fields[idx];
            DataType fieldType = field.dataType();
            fieldTypes[idx] = new FakeUnsafeGenericType(rowWriter, fieldType);
        }
    }

    @Override
    public void init() throws UnsupportedDataTypeException {
        for (int idx = 0; idx < this.fields.length; idx++) {
            fieldTypes[idx].init();
        }
    }

    @Override
    public void genIntoWriter(int ordinal) throws UnsupportedDataTypeException {
        if (!isInner) {
            this.gen();
            return;
        }

        int prevCursor = writer().cursor();
        this.rowWriter.resetRowWriter();
        for (int idx = 0; idx < this.fields.length; idx++) {
            fieldTypes[idx].genIntoWriter(idx);
        }
        this.writer().setOffsetAndSizeFromPreviousCursor(ordinal, prevCursor);
    }

    public void gen() throws UnsupportedDataTypeException {
        for (int idx = 0; idx < this.fields.length; idx++) {
            fieldTypes[idx].genIntoWriter(idx);
        }
    }

    @Override
    public void constructCode(Context ctx) {
        String writerName = ctx.getNameOrCreate(this.writer());
        String rowWriterClassName = UnsafeRowWriter.class.getName();
        String rowWriterName = ctx.getNameOrCreate(this.rowWriter);
        ctx.addFieldDeclare(rowWriterClassName + " " + rowWriterName + ";");
        if (isInner) {
            ctx.addIntoConstructor(
                    rowWriterName
                            + " = new "
                            + rowWriterClassName
                            + "("
                            + writerName
                            + ", "
                            + fields.length
                            + ");");
        } else {
            ctx.addIntoConstructor(rowWriterName + " = writer;");
        }

        for (int idx = 0; idx < fieldTypes.length; idx++) {
            fieldTypes[idx].constructCode(ctx);
        }
    }

    @Override
    public void initCode(Context ctx) {
        for (int idx = 0; idx < this.fields.length; idx++) {
            fieldTypes[idx].initCode(ctx);
        }
    }

    @Override
    public void genCode(Context ctx, String ordinalName) {
        if (isInner) {
            String prevCursorName = ctx.freshVarName();
            String writerName = ctx.getNameOrCreate(this.writer());
            String rowWriterName = ctx.getNameOrCreate(this.rowWriter);

            ctx.addIntoGen("int " + prevCursorName + " = " + writerName + ".cursor();");
            ctx.addIntoGen(rowWriterName + ".resetRowWriter();");

            for (int idx = 0; idx < fieldTypes.length; idx++) {
                fieldTypes[idx].genCode(ctx, String.valueOf(idx));
            }
            ctx.addIntoGen(
                    writerName
                            + ".setOffsetAndSizeFromPreviousCursor("
                            + ordinalName
                            + ", "
                            + prevCursorName
                            + ");");
        } else {
            for (int idx = 0; idx < fieldTypes.length; idx++) {
                fieldTypes[idx].genCode(ctx, String.valueOf(idx));
            }
        }
    }
}

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

package org.cheney.fake.generator.unsafe;

import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import org.cheney.fake.generator.code.FakeCodegen;
import org.cheney.fake.generator.code.FakeCodegen.Context;
import org.cheney.fake.generator.unsafe.type.FakeUnsafeRowType;

import javax.activation.UnsupportedDataTypeException;
import javax.validation.constraints.NotNull;

import java.util.concurrent.ExecutionException;

public class FakeUnsafeGenerator {

    private boolean enableCodegen = false;
    private final StructType schema;
    private final UnsafeRowWriter writer;
    private final FakeUnsafeRowType rowType;
    private String code;
    private IFakeGenerator generator;

    public FakeUnsafeGenerator(@NotNull StructType schema, boolean enableCodegen) {
        this.enableCodegen = enableCodegen;
        this.schema = schema;
        StructField[] fields = schema.fields();
        writer = new UnsafeRowWriter(fields.length, 96);
        rowType = new FakeUnsafeRowType(writer, schema);
    }

    public StructType getSchema() {
        return schema;
    }

    public UnsafeRowGenerator init() throws UnsupportedDataTypeException, ExecutionException {
        rowType.init();
        if (enableCodegen) {
            code = this.codegen();
            System.out.println("-------------------------");
            System.out.println(code);
            System.out.println("-------------------------");
            generator = FakeCodegen.Compiler.compile(code);
            generator.constructor(this.writer);
            generator.init();
        } else {
            generator =
                    new IFakeGenerator() {
                        @Override
                        public void constructor(UnsafeRowWriter writer) {}

                        @Override
                        public void init() {}

                        @Override
                        public void gen() throws UnsupportedDataTypeException {
                            rowType.gen();
                        }
                    };
        }

        return new UnsafeRowGenerator() {
            @Override
            public UnsafeRow gen() throws UnsupportedDataTypeException {
                writer.reset();
                writer.zeroOutNullBytes();
                generator.gen();
                return writer.getRow();
            }
        };
    }

    private String codegen() throws UnsupportedDataTypeException {
        Context ctx = new Context();
        this.rowType.constructCode(ctx);
        String fieldDeclareCode = ctx.fieldDeclare();
        String constructCode = ctx.constructor();

        this.rowType.initCode(ctx);
        String initCode = ctx.init();

        this.rowType.genCode(ctx, "0");
        String genCode = ctx.gen();

        String code =
                "public "
                        + IFakeGenerator.class.getName()
                        + " create() { \n"
                        + "return new OneFakeGenerator();"
                        + "}\n"
                        + "class OneFakeGenerator implements "
                        + IFakeGenerator.class.getName()
                        + " {\n"
                        + "\n"
                        + fieldDeclareCode
                        + "\n"
                        + "  @Override\n"
                        + "  public void constructor(org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter writer) {\n"
                        + constructCode
                        + "\n"
                        + "  }\n"
                        + "\n"
                        + "  @Override\n"
                        + "  public void init() {\n"
                        + initCode
                        + "\n"
                        + "  }\n"
                        + "\n"
                        + "  @Override\n"
                        + "  public void gen() throws "
                        + UnsupportedDataTypeException.class.getName()
                        + " {\n"
                        + genCode
                        + "\n"
                        + "  }\n"
                        + "}\n";
        return code;
    }
}

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

package org.sfaker.generator.code;

import org.sfaker.generator.unsafe.IFakeGenerator;

import org.apache.spark.TaskContext;
import org.apache.spark.TaskKilledException;
import org.apache.spark.executor.InputMetrics;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.UnsafeArrayData;
import org.apache.spark.sql.catalyst.expressions.UnsafeMapData;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeArrayWriter;
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter;
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeWriter;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.errors.QueryExecutionErrors;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;
import org.apache.spark.util.ParentClassLoader;
import org.apache.spark.util.Utils;

import org.codehaus.commons.compiler.CompileException;
import org.codehaus.janino.ClassBodyEvaluator;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public interface FakeCodegen {
    public static class Compiler {
        private static final CacheLoader<String, IFakeGenerator.Creator> cacheLoader =
                new CacheLoader<String, IFakeGenerator.Creator>() {
                    @Override
                    public IFakeGenerator.Creator load(String code) throws Exception {
                        return doCompile(code);
                    }
                };
        private static final LoadingCache<String, IFakeGenerator.Creator> cache =
                CacheBuilder.newBuilder().build(cacheLoader);

        public static IFakeGenerator compile(String code) throws ExecutionException {
            return cache.get(code).create();
        }

        public static IFakeGenerator.Creator doCompile(String code) {
            ClassBodyEvaluator evaluator = new ClassBodyEvaluator();
            ParentClassLoader parentClassLoader =
                    new ParentClassLoader(Utils.getContextOrSparkClassLoader());
            evaluator.setParentClassLoader(parentClassLoader);

            evaluator.setClassName("org.cheney.fake.generator.code.FakeGeneratorCreator");
            evaluator.setDefaultImports(
                    IFakeGenerator.class.getName(),
                    Platform.class.getName(),
                    InternalRow.class.getName(),
                    UnsafeRow.class.getName(),
                    UTF8String.class.getName(),
                    Decimal.class.getName(),
                    CalendarInterval.class.getName(),
                    ArrayData.class.getName(),
                    UnsafeArrayData.class.getName(),
                    MapData.class.getName(),
                    UnsafeMapData.class.getName(),
                    Expression.class.getName(),
                    TaskContext.class.getName(),
                    TaskKilledException.class.getName(),
                    InputMetrics.class.getName(),
                    QueryExecutionErrors.class.getName().strip().replaceAll("\\$$", ""));
            evaluator.setExtendedClass(IFakeGenerator.Creator.class);

            try {
                evaluator.cook("fake.java", code);
                return (IFakeGenerator.Creator) evaluator.getClazz().getConstructor().newInstance();
            } catch (CompileException
                    | InvocationTargetException
                    | InstantiationException
                    | IllegalAccessException
                    | NoSuchMethodException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static class Context {
        private StringBuilder builder = new StringBuilder();
        private StringBuilder fieldDeclare = new StringBuilder();
        private StringBuilder constructor = new StringBuilder();
        private StringBuilder init = new StringBuilder();
        private StringBuilder gen = new StringBuilder();
        private Map<UnsafeWriter, String> names = new HashMap<>();

        private int varCounter = 0;

        public Context addFieldDeclare(String fieldDeclare) {
            this.fieldDeclare.append(fieldDeclare).append("\n");
            return this;
        }

        public Context addIntoConstructor(String statement) {
            this.constructor.append(statement).append("\n");
            return this;
        }

        public Context addIntoInit(String statement) {
            this.init.append(statement).append("\n");
            return this;
        }

        public Context addIntoGen(String statement) {
            this.gen.append(statement).append("\n");
            return this;
        }

        public String fieldDeclare() {
            return this.fieldDeclare.toString();
        }

        public String constructor() {
            return this.constructor.toString();
        }

        public String init() {
            return this.init.toString();
        }

        public String gen() {
            return this.gen.toString();
        }

        public String stage() {
            String stage = builder.toString();
            this.builder = new StringBuilder();
            return stage;
        }

        public String freshVarName() {
            return "tmp_var_" + varCounter++;
        }

        public String getNameOrCreate(UnsafeWriter writer) {
            if (names.containsKey(writer)) {
                return names.get(writer);
            }

            String name = "";
            if (writer instanceof UnsafeArrayWriter) {
                name = "unsafe_array_writer_" + names.size();
            } else if (writer instanceof UnsafeRowWriter) {
                name = "unsafe_row_writer_" + names.size();
            } else {
                name = "unsafe_writer_" + names.size();
            }
            names.put(writer, name);
            return name;
        }
    }

    public void constructCode(Context ctx);

    public void initCode(Context ctx);

    public void genCode(Context ctx, String ordinalName);
}

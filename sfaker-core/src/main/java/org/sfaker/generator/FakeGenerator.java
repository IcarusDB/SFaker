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

package org.sfaker.generator;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;

import scala.collection.immutable.Seq;
import scala.jdk.javaapi.CollectionConverters;

import javax.activation.UnsupportedDataTypeException;

import java.math.BigDecimal;
import java.util.List;

public class FakeGenerator {

    public static InternalRow gen(StructType schema) throws UnsupportedDataTypeException {
        StructField[] fields = schema.fields();
        Object[] row = new Object[fields.length];

        for (int ordinal = 0; ordinal < fields.length; ordinal++) {
            StructField field = fields[ordinal];
            write(field.dataType(), row, ordinal);
        }
        return InternalRow.fromSeq(CollectionConverters.asScala(List.of(row)).toSeq());
    }

    private static void write(DataType fieldType, Object[] row, int ordinal)
            throws UnsupportedDataTypeException {
        if (fieldType == DataTypes.ByteType) {
            byte val = genByte();
            row[ordinal] = val;
            return;
        }

        if (fieldType == DataTypes.ShortType) {
            short val = genShort();
            row[ordinal] = val;
            return;
        }

        if (fieldType == DataTypes.IntegerType) {
            int val = genInt();
            row[ordinal] = val;
            return;
        }

        if (fieldType == DataTypes.LongType) {
            long val = genLong();
            row[ordinal] = val;
            return;
        }

        if (fieldType == DataTypes.FloatType) {
            float val = genFloat();
            row[ordinal] = val;
            return;
        }
        if (fieldType == DataTypes.DoubleType) {
            double val = genDouble();
            row[ordinal] = val;
            return;
        }

        /*
        Decimal
         */

        if (fieldType == DataTypes.BooleanType) {
            boolean val = genBoolean();
            row[ordinal] = val;
            return;
        }

        if (fieldType == DataTypes.StringType) {
            String val = genString();
            row[ordinal] = UTF8String.fromString(val);
            return;
        }

        if (fieldType instanceof ArrayType) {
            DataType elementType = ((ArrayType) fieldType).elementType();
            int elementSize = 8;
            Object[] array = new Object[elementSize];
            for (int idx = 0; idx < elementSize; idx++) {
                write(elementType, array, idx);
            }

            row[ordinal] =
                    ArrayData.toArrayData(
                            SeqToArray.copy(CollectionConverters.asScala(List.of(array)).toSeq()));
            return;
        }

        if (fieldType instanceof MapType) {
            DataType keyType = ((MapType) fieldType).keyType();
            DataType valueType = ((MapType) fieldType).valueType();
            int entrySize = 3;
            Object[] key = new Object[entrySize];
            Object[] value = new Object[entrySize];
            for (int idx = 0; idx < entrySize; idx++) {
                write(keyType, key, idx);
                write(valueType, value, idx);
            }

            Seq<Object> keySeq = CollectionConverters.asScala(List.of(key)).toSeq();
            Seq<Object> valueSeq = CollectionConverters.asScala(List.of(value)).toSeq();
            MapData mapData = CreateMapData.<Object, Object>create(keySeq, valueSeq);
            row[ordinal] = mapData;
            return;
        }

        if (fieldType instanceof StructType) {
            StructType fieldSchema = (StructType) fieldType;
            InternalRow fieldRow = gen(fieldSchema);
            row[ordinal] = fieldRow;
            return;
        }

        throw new UnsupportedDataTypeException("Fail to generate a row.");
    }

    public static int genInt() {
        return RandomUtils.nextInt();
    }

    public static boolean genBoolean() {
        return RandomUtils.nextBoolean();
    }

    public static byte genByte() {
        return RandomUtils.nextBytes(1)[0];
    }

    public static short genShort() {
        return (short) genInt();
    }

    public static long genLong() {
        return RandomUtils.nextLong();
    }

    public static float genFloat() {
        return RandomUtils.nextFloat();
    }

    public static double genDouble() {
        return RandomUtils.nextDouble();
    }

    public static BigDecimal genDecimal() {
        return new BigDecimal(genLong());
    }

    public static String genString() {
        return RandomStringUtils.randomAscii(8);
    }
}

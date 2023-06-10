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

package org.sfaker.source;

import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.Map;

public class FakeSource implements DataSourceRegister, TableProvider {

    public static final String FAKE_SOURCE = "FakeSource";

    public FakeSource() {}

    @Override
    public StructType inferSchema(CaseInsensitiveStringMap options) {
        return null;
    }

    @Override
    public Table getTable(
            StructType schema, Transform[] partitioning, Map<String, String> properties) {
        String partitionsConf = properties.get(FakeSourceProps.CONF_PARTITIONS);
        String rowsTotalSizeConf = properties.get(FakeSourceProps.CONF_ROWS_TOTAL_SIZE);
        String unsafeRowEnableConf = properties.get(FakeSourceProps.CONF_UNSAFE_ROW_ENABLE);
        String unsafeCodegenEnableConf = properties.get(FakeSourceProps.CONF_UNSAFE_CODEGEN_ENABLE);

        int partitions = partitionsConf == null ? 1 : Integer.parseInt(partitionsConf);
        long rowsTotalSize = rowsTotalSizeConf == null ? 8 : Long.parseLong(rowsTotalSizeConf);
        boolean unsafeRowEnable =
                unsafeRowEnableConf == null ? false : Boolean.parseBoolean(unsafeRowEnableConf);
        boolean unsafeCodegenEnable =
                unsafeCodegenEnableConf == null
                        ? false
                        : Boolean.parseBoolean(unsafeCodegenEnableConf);

        return new FakeTable(
                new FakeSourceProps(
                        partitions, rowsTotalSize, unsafeRowEnable, unsafeCodegenEnable),
                schema);
    }

    @Override
    public String shortName() {
        return FAKE_SOURCE;
    }

    @Override
    public boolean supportsExternalMetadata() {
        return true;
    }
}

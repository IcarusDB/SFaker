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

package org.cheney.fake;

import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import org.cheney.fake.scan.FakeScanBuilder;

import java.util.Set;

public class FakeTable implements Table, SupportsRead {

    private final FakeSourceProps props;
    private final StructType schema;

    public FakeTable(FakeSourceProps props, StructType schema) {
        this.props = props;
        this.schema = schema;
    }

    public FakeSourceProps getProps() {
        return props;
    }

    @Override
    public String name() {
        return "FakeTable";
    }

    @Override
    public StructType schema() {
        return this.schema;
    }

    @Override
    public Set<TableCapability> capabilities() {
        return Set.of(TableCapability.BATCH_READ);
    }

    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
        return new FakeScanBuilder(props, this.schema);
    }
}

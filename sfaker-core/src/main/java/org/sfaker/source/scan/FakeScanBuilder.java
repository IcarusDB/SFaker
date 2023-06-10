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

package org.sfaker.source.scan;

import org.sfaker.source.FakeSourceProps;

import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.SupportsPushDownLimit;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.types.StructType;

public class FakeScanBuilder implements SupportsPushDownRequiredColumns, SupportsPushDownLimit {

    private FakeSourceProps props;
    private StructType schema;

    public FakeScanBuilder(FakeSourceProps props, StructType schema) {
        this.props = props;
        this.schema = schema;
    }

    public FakeSourceProps getProps() {
        return props;
    }

    @Override
    public Scan build() {
        return new FakeScan(this.props, this.schema);
    }

    @Override
    public void pruneColumns(StructType requiredSchema) {
        this.schema = this.schema.copy(requiredSchema.fields());
    }

    @Override
    public boolean pushLimit(int limit) {
        this.props =
                new FakeSourceProps(
                        this.props.getPartitions(),
                        limit,
                        this.props.isUnsafeRowEnable(),
                        this.props.isUnsafeCodegenEnable());
        return true;
    }
}

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

package org.sfaker.source.batch;

import org.sfaker.source.FakeSourceProps;
import org.sfaker.source.batch.partition.FakeBatchInputPartition;
import org.sfaker.source.batch.partition.FakeBatchPartitionReaderFactory;
import org.sfaker.source.batch.partition.FakeBatchPartitionUnsafeReaderFactory;

import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.types.StructType;

import java.util.stream.IntStream;

public class FakeBatch implements Batch {

    private final FakeSourceProps props;
    private final StructType schema;

    public FakeBatch(FakeSourceProps props, StructType schema) {
        this.props = props;
        this.schema = schema;
    }

    public FakeSourceProps getProps() {
        return props;
    }

    public StructType getSchema() {
        return schema;
    }

    @Override
    public InputPartition[] planInputPartitions() {
        long rowsTotalSize = this.props.getRowsTotalSize();
        int partitions = this.props.getPartitions();
        long perPartitionRows = rowsTotalSize / partitions;
        long remain = rowsTotalSize % partitions;

        return IntStream.range(0, partitions)
                .<FakeBatchInputPartition>mapToObj(
                        (int id) -> {
                            if (id == partitions - 1) {
                                return new FakeBatchInputPartition(
                                        id, remain + perPartitionRows, schema);
                            }
                            return new FakeBatchInputPartition(id, perPartitionRows, schema);
                        })
                .toArray(InputPartition[]::new);
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        if (this.props.isUnsafeRowEnable()) {
            return new FakeBatchPartitionUnsafeReaderFactory(
                    this.schema, this.props.isUnsafeCodegenEnable());
        }
        return new FakeBatchPartitionReaderFactory();
    }
}

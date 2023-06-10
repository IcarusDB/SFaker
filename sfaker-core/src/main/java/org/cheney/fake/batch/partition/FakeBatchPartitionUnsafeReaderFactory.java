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

package org.cheney.fake.batch.partition;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.types.StructType;

public class FakeBatchPartitionUnsafeReaderFactory implements PartitionReaderFactory {

    private final StructType schema;
    private boolean enableCodegen;

    public FakeBatchPartitionUnsafeReaderFactory(StructType schema, boolean enableCodegen) {
        this.schema = schema;
        this.enableCodegen = enableCodegen;
    }

    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
        if (partition instanceof FakeBatchInputPartition) {
            FakeBatchPartitionUnsafeReader reader =
                    new FakeBatchPartitionUnsafeReader(
                            (FakeBatchInputPartition) partition, this.enableCodegen);
            return reader;
        }
        return null;
    }
}

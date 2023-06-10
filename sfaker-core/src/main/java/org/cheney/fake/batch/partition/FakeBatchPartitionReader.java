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
import org.apache.spark.sql.connector.read.PartitionReader;

import org.cheney.fake.generator.FakeGenerator;

import javax.activation.UnsupportedDataTypeException;
import javax.validation.constraints.NotNull;

import java.io.IOException;

public class FakeBatchPartitionReader implements PartitionReader<InternalRow> {

    private final FakeBatchInputPartition inputPartition;
    private long counter = 0;

    public FakeBatchPartitionReader(@NotNull FakeBatchInputPartition inputPartition) {
        this.inputPartition = inputPartition;
    }

    @Override
    public boolean next() throws IOException {
        return counter < inputPartition.getRowsTotalSize();
    }

    @Override
    public InternalRow get() {
        try {
            InternalRow row = null;
            row = FakeGenerator.gen(this.inputPartition.getSchema());
            this.counter++;
            return row;
        } catch (UnsupportedDataTypeException e) {
            throw new RuntimeException(e);
        }
    }

    public long getCounter() {
        return counter;
    }

    @Override
    public void close() throws IOException {}
}

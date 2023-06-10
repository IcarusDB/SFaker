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

import org.cheney.fake.generator.unsafe.FakeUnsafeGenerator;
import org.cheney.fake.generator.unsafe.UnsafeRowGenerator;

import javax.activation.UnsupportedDataTypeException;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

public class FakeBatchPartitionUnsafeReader implements PartitionReader<InternalRow> {

    private final FakeBatchInputPartition inputPartition;
    private final UnsafeRowGenerator rowGenerator;
    private long counter = 0;

    public FakeBatchPartitionUnsafeReader(
            FakeBatchInputPartition inputPartition, boolean enableCodegen) {
        this.inputPartition = inputPartition;
        FakeUnsafeGenerator generator =
                new FakeUnsafeGenerator(this.inputPartition.getSchema(), enableCodegen);
        try {
            this.rowGenerator = generator.init();
        } catch (UnsupportedDataTypeException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean next() throws IOException {
        return counter < inputPartition.getRowsTotalSize();
    }

    @Override
    public InternalRow get() {
        InternalRow row = null;
        try {
            row = this.rowGenerator.gen();
        } catch (UnsupportedDataTypeException e) {
            throw new RuntimeException(e);
        }
        this.counter++;
        return row;
    }

    public long getCounter() {
        return counter;
    }

    @Override
    public void close() throws IOException {
        System.out.println("counter: " + counter);
    }
}

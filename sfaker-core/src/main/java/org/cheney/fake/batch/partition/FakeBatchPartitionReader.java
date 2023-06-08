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

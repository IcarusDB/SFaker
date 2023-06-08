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

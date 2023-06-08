package org.cheney.fake.batch.partition;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;

public class FakeBatchPartitionReaderFactory implements PartitionReaderFactory {

    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition)
            throws UnsupportedOperationException {
        if (partition instanceof FakeBatchInputPartition) {
            return new FakeBatchPartitionReader((FakeBatchInputPartition) partition);
        }
        throw new UnsupportedOperationException(
                String.format(
                        "%s is not instance of %s",
                        partition.getClass(), FakeBatchInputPartition.class));
    }
}

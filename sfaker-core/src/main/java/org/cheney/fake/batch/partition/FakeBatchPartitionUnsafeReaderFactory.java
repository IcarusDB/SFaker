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

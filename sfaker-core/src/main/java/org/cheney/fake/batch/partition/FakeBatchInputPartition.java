package org.cheney.fake.batch.partition;

import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.types.StructType;

public class FakeBatchInputPartition implements InputPartition {

    private final long partitionId;
    private final long rowsTotalSize;
    private final StructType schema;

    public FakeBatchInputPartition(long partitionId, long rowsTotalSize, StructType schema) {
        this.partitionId = partitionId;
        this.rowsTotalSize = rowsTotalSize;
        this.schema = schema;
    }

    public long getPartitionId() {
        return partitionId;
    }

    public long getRowsTotalSize() {
        return rowsTotalSize;
    }

    public StructType getSchema() {
        return schema;
    }
}

package org.cheney.fake.batch;

import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.types.StructType;

import org.cheney.fake.FakeSourceProps;
import org.cheney.fake.batch.partition.FakeBatchInputPartition;
import org.cheney.fake.batch.partition.FakeBatchPartitionReaderFactory;
import org.cheney.fake.batch.partition.FakeBatchPartitionUnsafeReaderFactory;

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

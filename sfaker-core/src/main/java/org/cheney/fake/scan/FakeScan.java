package org.cheney.fake.scan;

import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.types.StructType;

import org.cheney.fake.FakeSourceProps;
import org.cheney.fake.batch.FakeBatch;

public class FakeScan implements Scan {

    private final FakeSourceProps props;
    private final StructType schema;

    public FakeScan(FakeSourceProps props, StructType schema) {
        this.props = props;
        this.schema = schema;
    }

    public FakeSourceProps getProps() {
        return props;
    }

    @Override
    public StructType readSchema() {
        return schema;
    }

    @Override
    public Batch toBatch() {
        return new FakeBatch(props, schema);
    }
}

package org.cheney.fake.scan;

import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.SupportsPushDownLimit;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.types.StructType;

import org.cheney.fake.FakeSourceProps;

public class FakeScanBuilder implements SupportsPushDownRequiredColumns, SupportsPushDownLimit {

    private FakeSourceProps props;
    private StructType schema;

    public FakeScanBuilder(FakeSourceProps props, StructType schema) {
        this.props = props;
        this.schema = schema;
    }

    public FakeSourceProps getProps() {
        return props;
    }

    @Override
    public Scan build() {
        return new FakeScan(this.props, this.schema);
    }

    @Override
    public void pruneColumns(StructType requiredSchema) {
        this.schema = this.schema.copy(requiredSchema.fields());
    }

    @Override
    public boolean pushLimit(int limit) {
        this.props =
                new FakeSourceProps(
                        this.props.getPartitions(),
                        limit,
                        this.props.isUnsafeRowEnable(),
                        this.props.isUnsafeCodegenEnable());
        return true;
    }
}

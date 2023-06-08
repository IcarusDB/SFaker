package org.cheney.fake;

import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import org.cheney.fake.scan.FakeScanBuilder;

import java.util.Set;

public class FakeTable implements Table, SupportsRead {

    private final FakeSourceProps props;
    private final StructType schema;

    public FakeTable(FakeSourceProps props, StructType schema) {
        this.props = props;
        this.schema = schema;
    }

    public FakeSourceProps getProps() {
        return props;
    }

    @Override
    public String name() {
        return "FakeTable";
    }

    @Override
    public StructType schema() {
        return this.schema;
    }

    @Override
    public Set<TableCapability> capabilities() {
        return Set.of(TableCapability.BATCH_READ);
    }

    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
        return new FakeScanBuilder(props, this.schema);
    }
}

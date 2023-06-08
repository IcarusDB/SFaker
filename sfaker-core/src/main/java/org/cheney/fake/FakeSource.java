package org.cheney.fake;

import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.Map;

public class FakeSource implements DataSourceRegister, TableProvider {

    public static final String FAKE_SOURCE = "FakeSource";

    public FakeSource() {}

    @Override
    public StructType inferSchema(CaseInsensitiveStringMap options) {
        return null;
    }

    @Override
    public Table getTable(
            StructType schema, Transform[] partitioning, Map<String, String> properties) {
        String partitionsConf = properties.get(FakeSourceProps.CONF_PARTITIONS);
        String rowsTotalSizeConf = properties.get(FakeSourceProps.CONF_ROWS_TOTAL_SIZE);
        String unsafeRowEnableConf = properties.get(FakeSourceProps.CONF_UNSAFE_ROW_ENABLE);
        String unsafeCodegenEnableConf = properties.get(FakeSourceProps.CONF_UNSAFE_CODEGEN_ENABLE);

        int partitions = partitionsConf == null ? 1 : Integer.parseInt(partitionsConf);
        long rowsTotalSize = rowsTotalSizeConf == null ? 8 : Long.parseLong(rowsTotalSizeConf);
        boolean unsafeRowEnable =
                unsafeRowEnableConf == null ? false : Boolean.parseBoolean(unsafeRowEnableConf);
        boolean unsafeCodegenEnable =
                unsafeCodegenEnableConf == null
                        ? false
                        : Boolean.parseBoolean(unsafeCodegenEnableConf);

        return new FakeTable(
                new FakeSourceProps(
                        partitions, rowsTotalSize, unsafeRowEnable, unsafeCodegenEnable),
                schema);
    }

    @Override
    public String shortName() {
        return FAKE_SOURCE;
    }

    @Override
    public boolean supportsExternalMetadata() {
        return true;
    }
}

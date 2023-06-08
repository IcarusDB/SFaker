package org.cheney.fake;

public class FakeSourceProps {

    public static final String CONF_ROWS_TOTAL_SIZE = "spark.sql.fake.source.rowsTotalSize";
    public static final String CONF_PARTITIONS = "spark.sql.fake.source.partitions";
    public static final String CONF_UNSAFE_ROW_ENABLE = "spark.sql.fake.source.unsafe.row.enable";

    public static final String CONF_UNSAFE_CODEGEN_ENABLE =
            "spark.sql.fake.source.unsafe.codegen.enable";
    private final int partitions;
    private final long rowsTotalSize;
    private final boolean unsafeRowEnable;
    private final boolean unsafeCodegenEnable;

    public FakeSourceProps(
            int partitions,
            long rowsTotalSize,
            boolean unsafeRowEnable,
            boolean unsafeCodegenEnable) {
        this.partitions = partitions;
        this.rowsTotalSize = rowsTotalSize;
        this.unsafeRowEnable = unsafeRowEnable;
        this.unsafeCodegenEnable = unsafeCodegenEnable;
    }

    public int getPartitions() {
        return partitions;
    }

    public long getRowsTotalSize() {
        return rowsTotalSize;
    }

    public boolean isUnsafeRowEnable() {
        return unsafeRowEnable;
    }

    public boolean isUnsafeCodegenEnable() {
        return unsafeCodegenEnable;
    }
}

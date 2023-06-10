/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.sfaker.source;

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

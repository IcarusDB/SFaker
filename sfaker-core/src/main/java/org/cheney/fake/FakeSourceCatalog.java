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

package org.cheney.fake;

import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.catalog.DelegatingCatalogExtension;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.V1Table;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;

import java.util.Map;

public class FakeSourceCatalog extends DelegatingCatalogExtension {
    //  Cache<Identifier, Table> cache = CacheBuilder.newBuilder().build();

    //  @Override
    //  public Table createTable(Identifier ident, StructType schema, Transform[] partitions,
    //      Map<String, String> properties) throws TableAlreadyExistsException,
    // NoSuchNamespaceException {
    //      String provider = properties.getOrDefault(TableCatalog.PROP_PROVIDER, "");
    //      if (provider.equals("")) {
    //        return super.createTable(ident, schema, partitions, properties);
    //      }
    //
    //      if (!provider.toLowerCase().strip().equals(FakeSource.FAKE_SOURCE.toLowerCase())) {
    //        return super.createTable(ident, schema, partitions, properties);
    //      }
    //
    //      if (cache.getIfPresent(ident) != null) {
    //        throw new TableAlreadyExistsException(ident);
    //      }
    //      Table table = new FakeSource().getTable(schema, partitions, properties);
    //      cache.put(ident, table);
    //
    //      super.createTable(ident, schema, partitions, properties);
    //
    //      return table;
    //  }

    private Table tryConvertTable(Table table) {
        if (table instanceof V1Table) {
            V1Table v1Table = (V1Table) table;
            Map<String, String> properties = v1Table.properties();
            String provider = properties.getOrDefault(TableCatalog.PROP_PROVIDER, "");

            if (provider.toLowerCase().strip().equals(FakeSource.FAKE_SOURCE.toLowerCase())) {
                StructType schema = v1Table.schema();
                Transform[] partitions = v1Table.partitioning();
                return new FakeSource().getTable(schema, partitions, properties);
            }
        }
        return table;
    }

    @Override
    public Table loadTable(Identifier ident) throws NoSuchTableException {
        Table table = super.loadTable(ident);
        return tryConvertTable(table);
    }

    @Override
    public Table loadTable(Identifier ident, long timestamp) throws NoSuchTableException {
        Table table = super.loadTable(ident, timestamp);
        return tryConvertTable(table);
    }

    @Override
    public Table loadTable(Identifier ident, String version) throws NoSuchTableException {
        Table table = super.loadTable(ident, version);
        return tryConvertTable(table);
    }
}

/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.prestosql.orc;

import com.google.common.collect.ImmutableList;
import io.prestosql.memory.context.AggregatedMemoryContext;
import io.prestosql.orc.metadata.OrcColumnId;
import io.prestosql.orc.metadata.OrcType;
import io.prestosql.orc.reader.ColumnReaders;
import io.prestosql.spi.type.VarcharType;
import org.testng.annotations.Test;

public class TestAbstractNumbericColumnReader
{
    private VarcharType type = VarcharType.VARCHAR;

    @Test (expectedExceptions = OrcCorruptionException.class)
    public void testTypeCoercionShort()
            throws OrcCorruptionException
    {
        OrcColumn column = new OrcColumn(
                "hdfs://hacluster/user/hive/warehouse/tpcds_orc_hive_1000.db/catalog_sales/cs_sold_date_sk=2452268/000896_0",
                new OrcColumnId(3),
                "cs_order_number",
                OrcType.OrcTypeKind.SHORT,
                new OrcDataSourceId("hdfs://hacluster/user/hive/warehouse/tpcds_orc_hive_1000.db/catalog_sales/cs_sold_date_sk=2452268/000896_0"),
                ImmutableList.of());
        ColumnReaders.createColumnReader(type, column, AggregatedMemoryContext.newSimpleAggregatedMemoryContext(), null);
    }

    @Test (expectedExceptions = OrcCorruptionException.class)
    public void testTypeCoercionInteger()
            throws OrcCorruptionException
    {
        OrcColumn column = new OrcColumn(
                "hdfs://hacluster/user/hive/warehouse/tpcds_orc_hive_1000.db/catalog_sales/cs_sold_date_sk=2452268/000896_0",
                new OrcColumnId(3),
                "cs_order_number",
                OrcType.OrcTypeKind.INT,
                new OrcDataSourceId("hdfs://hacluster/user/hive/warehouse/tpcds_orc_hive_1000.db/catalog_sales/cs_sold_date_sk=2452268/000896_0"),
                ImmutableList.of());
        ColumnReaders.createColumnReader(type, column, AggregatedMemoryContext.newSimpleAggregatedMemoryContext(), null);
    }

    @Test (expectedExceptions = OrcCorruptionException.class)
    public void testTypeCoercionBigInt()
            throws OrcCorruptionException
    {
        OrcColumn column = new OrcColumn(
                "hdfs://hacluster/user/hive/warehouse/tpcds_orc_hive_1000.db/catalog_sales/cs_sold_date_sk=2452268/000896_0",
                new OrcColumnId(3),
                "cs_order_number",
                OrcType.OrcTypeKind.LONG,
                new OrcDataSourceId("hdfs://hacluster/user/hive/warehouse/tpcds_orc_hive_1000.db/catalog_sales/cs_sold_date_sk=2452268/000896_0"),
                ImmutableList.of());
        ColumnReaders.createColumnReader(type, column, AggregatedMemoryContext.newSimpleAggregatedMemoryContext(), null);
    }

    @Test (expectedExceptions = OrcCorruptionException.class)
    public void testTypeCoercionDate()
            throws OrcCorruptionException
    {
        OrcColumn column = new OrcColumn(
                "hdfs://hacluster/user/hive/warehouse/tpcds_orc_hive_1000.db/catalog_sales/cs_sold_date_sk=2452268/000896_0",
                new OrcColumnId(3),
                "cs_order_number",
                OrcType.OrcTypeKind.DATE,
                new OrcDataSourceId("hdfs://hacluster/user/hive/warehouse/tpcds_orc_hive_1000.db/catalog_sales/cs_sold_date_sk=2452268/000896_0"),
                ImmutableList.of());
        ColumnReaders.createColumnReader(type, column, AggregatedMemoryContext.newSimpleAggregatedMemoryContext(), null);
    }
}

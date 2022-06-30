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
package io.prestosql.plugin.pmemory;

import io.prestosql.spi.connector.*;

public final class PMemoryHandleResolver
        implements ConnectorHandleResolver
{
    @Override
    public Class<? extends ConnectorTableHandle> getTableHandleClass()
    {
        return PMemoryTableHandle.class;
    }

    @Override
    public Class<? extends ColumnHandle> getColumnHandleClass()
    {
        return PMemoryColumnHandle.class;
    }

    @Override
    public Class<? extends ConnectorSplit> getSplitClass()
    {
        return PMemorySplitWrapper.class;
    }

    @Override
    public Class<? extends ConnectorOutputTableHandle> getOutputTableHandleClass()
    {
        return PMemoryOutputTableHandle.class;
    }

    @Override
    public Class<? extends ConnectorInsertTableHandle> getInsertTableHandleClass()
    {
        return PMemoryInsertTableHandle.class;
    }

    @Override
    public Class<? extends ConnectorTransactionHandle> getTransactionHandleClass()
    {
        return PMemoryTransactionHandle.class;
    }

    @Override
    public Class<? extends ConnectorPartitioningHandle> getPartitioningHandleClass()
    {
        return PMemoryPartitioningHandle.class;
    }
}

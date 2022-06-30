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

import com.google.common.collect.ImmutableList;
import io.prestosql.spi.connector.*;
import io.prestosql.spi.session.PropertyMetadata;
import io.prestosql.spi.transaction.IsolationLevel;

import javax.inject.Inject;
import java.util.List;

import static java.util.Objects.requireNonNull;

//extend connector with methods for NodePartitioningProvider and tableProperties
public class PMemoryConnector
        implements Connector
{
    private final PMemoryMetadata metadata;
    private final PMemorySplitManager splitManager;
    private final PMemoryPageSourceProvider pageSourceProvider;
    private final PMemoryPageSinkProvider pageSinkProvider;
    private final ConnectorNodePartitioningProvider nodePartitioningProvider;

    private final List<PropertyMetadata<?>> tableProperties;

    @Inject
    public PMemoryConnector(
            PMemoryMetadata metadata,
            PMemorySplitManager splitManager,
            PMemoryPageSourceProvider pageSourceProvider,
            PMemoryPageSinkProvider pageSinkProvider,
            PMemoryNodePartitioningProvider nodePartitioningProvider,
            PMemoryTableProperties tableProperties
    )
    {
        this.metadata = metadata;
        this.splitManager = splitManager;
        this.pageSourceProvider = pageSourceProvider;
        this.pageSinkProvider = pageSinkProvider;
        this.nodePartitioningProvider = nodePartitioningProvider;
        this.tableProperties = tableProperties.getTableProperties();

        nodePartitioningProvider.setMetadataHandle(metadata);
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly)
    {
        return PMemoryTransactionHandle.INSTANCE;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorTransactionHandle transactionHandle)
    {
        return metadata;
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return splitManager;
    }

    @Override
    public ConnectorPageSourceProvider getPageSourceProvider()
    {
        return pageSourceProvider;
    }

    @Override
    public ConnectorPageSinkProvider getPageSinkProvider()
    {
        return pageSinkProvider;
    }

    @Override
    public List<PropertyMetadata<?>> getTableProperties()
    {
        return tableProperties;
    }

    @Override
    public ConnectorNodePartitioningProvider getNodePartitioningProvider()
    {
        return nodePartitioningProvider;
    }
}

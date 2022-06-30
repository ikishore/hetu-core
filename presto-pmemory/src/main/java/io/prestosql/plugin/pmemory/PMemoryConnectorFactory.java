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

import com.google.common.collect.ImmutableSet;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.json.JsonModule;
import io.prestosql.spi.NodeManager;
import io.prestosql.spi.PageIndexerFactory;
import io.prestosql.spi.PageSorter;
import io.prestosql.spi.VersionEmbedder;
import io.prestosql.spi.connector.*;
import io.prestosql.spi.connector.classloader.ClassLoaderSafeConnectorPageSinkProvider;
import io.prestosql.spi.connector.classloader.ClassLoaderSafeConnectorPageSourceProvider;
import io.prestosql.spi.connector.classloader.ClassLoaderSafeConnectorSplitManager;
import io.prestosql.spi.connector.classloader.ClassLoaderSafeNodePartitioningProvider;
import io.prestosql.spi.heuristicindex.IndexClient;
import io.prestosql.spi.procedure.Procedure;
import io.prestosql.spi.type.TypeManager;

import java.util.Map;
import java.util.Set;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static java.util.Objects.requireNonNull;

public class PMemoryConnectorFactory
        implements ConnectorFactory
{
    ClassLoader classLoader;

    public PMemoryConnectorFactory(ClassLoader classLoader)
    {
        this.classLoader = classLoader;
    }

    @Override
    public String getName()
    {
        return "pmemory";
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return new PMemoryHandleResolver();
    }

    @Override
    public Connector create(String catalogName, Map<String, String> requiredConfig, ConnectorContext context)
    {
        requireNonNull(requiredConfig, "requiredConfig is null");
        try {
            // A plugin is not required to use Guice; it is just very convenient
            Bootstrap app = new Bootstrap(
                    new JsonModule(),
                    new PMemoryModule(context.getTypeManager(), context.getNodeManager()),
                    binder -> {
                        binder.bind(PageIndexerFactory.class).toInstance(context.getPageIndexerFactory());
                    });

            Injector injector = app
                    .strictConfig()
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(requiredConfig)
                    .initialize();

            LifeCycleManager lifeCycleManager = injector.getInstance(LifeCycleManager.class);
            PMemoryMetadata metadata = injector.getInstance(PMemoryMetadata.class);
            PMemorySplitManager splitManager = injector.getInstance(PMemorySplitManager.class);
            PMemoryPageSourceProvider connectorPageSource = injector.getInstance(PMemoryPageSourceProvider.class);
            PMemoryPageSinkProvider pageSinkProvider = injector.getInstance(PMemoryPageSinkProvider.class);
            PMemoryNodePartitioningProvider connectorDistributionProvider = injector.getInstance(PMemoryNodePartitioningProvider.class);
            PMemoryTableProperties pMemoryTableProperties = injector.getInstance(PMemoryTableProperties.class);

            return new PMemoryConnector(
                    metadata,
                    splitManager,
                    connectorPageSource,
                    pageSinkProvider,
                    connectorDistributionProvider,
                    pMemoryTableProperties);

            //return injector.getInstance(PMemoryConnector.class);
        }
        catch (Exception e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }
}

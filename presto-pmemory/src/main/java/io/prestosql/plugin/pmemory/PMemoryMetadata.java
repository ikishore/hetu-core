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

import com.google.common.collect.*;
import io.airlift.slice.Slice;
import io.prestosql.spi.HostAddress;
import io.prestosql.spi.Node;
import io.prestosql.spi.NodeManager;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorInsertTableHandle;
import io.prestosql.spi.connector.ConnectorMetadata;
import io.prestosql.spi.connector.ConnectorNewTableLayout;
import io.prestosql.spi.connector.ConnectorOutputMetadata;
import io.prestosql.spi.connector.ConnectorOutputTableHandle;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.ConnectorTablePartitioning;
import io.prestosql.spi.connector.ConnectorTableProperties;
import io.prestosql.spi.connector.ConnectorViewDefinition;
import io.prestosql.spi.connector.LimitApplicationResult;
import io.prestosql.spi.connector.SampleType;
import io.prestosql.spi.connector.SchemaNotFoundException;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.connector.SchemaTablePrefix;
import io.prestosql.spi.connector.ViewNotFoundException;
import io.prestosql.spi.statistics.ComputedStatistics;

import io.prestosql.spi.predicate.TupleDomain;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.spi.StandardErrorCode.*;
import static io.prestosql.spi.connector.SampleType.SYSTEM;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

@ThreadSafe
public class PMemoryMetadata
        implements ConnectorMetadata
{
    public static final String SCHEMA_NAME = "default";

    private final NodeManager nodeManager;
    private final List<String> schemas = new ArrayList<>();
    private final AtomicLong nextTableId = new AtomicLong();
    private final Map<SchemaTableName, Long> tableIds = new HashMap<>();
    private final Map<Long, TableInfo> tables = new HashMap<>();
    private final Map<SchemaTableName, ConnectorViewDefinition> views = new HashMap<>();

    @Inject
    public PMemoryMetadata(NodeManager nodeManager)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.schemas.add(SCHEMA_NAME);
    }

    @Override
    public synchronized List<String> listSchemaNames(ConnectorSession session)
    {
        return ImmutableList.copyOf(schemas);
    }

    @Override
    public synchronized void createSchema(ConnectorSession session, String schemaName, Map<String, Object> properties)
    {
        if (schemas.contains(schemaName)) {
            throw new PrestoException(ALREADY_EXISTS, format("Schema [%s] already exists", schemaName));
        }
        schemas.add(schemaName);
    }

    @Override
    public synchronized void dropSchema(ConnectorSession session, String schemaName)
    {
        if (!schemas.contains(schemaName)) {
            throw new PrestoException(NOT_FOUND, format("Schema [%s] does not exist", schemaName));
        }

        boolean tablesExist = tables.values().stream()
                .anyMatch(table -> table.getSchemaName().equals(schemaName));

        if (tablesExist) {
            throw new PrestoException(SCHEMA_NOT_EMPTY, "Schema not empty: " + schemaName);
        }

        verify(schemas.remove(schemaName));
    }

    @Override
    public Optional<Object> getInfo(ConnectorTableHandle table)
    {
        PMemoryTableHandle pMemoryTableHandle = (PMemoryTableHandle) table;
        Long id = pMemoryTableHandle.getId();
        Map<HostAddress, PMemoryDataFragment> fragments = tables.get(id).getDataFragments();
        Integer count = fragments.size();
        return Optional.of(count);
    }

    @Override
    public synchronized ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName schemaTableName)
    {
        Long id = tableIds.get(schemaTableName);
        if (id == null) {
            return null;
        }

        TableInfo table = tables.get(id);

        Optional<PMemoryBucketHandle> bucketHandle = getPMemoryBucketHandle(table);

        if (bucketHandle.isPresent()) {
            System.out.println("Table is partitioned");
        }

        return new PMemoryTableHandle(id, schemaTableName.getTableName(), bucketHandle);
    }

    public static Optional<PMemoryBucketHandle> getPMemoryBucketHandle(TableInfo table)
    {
        Optional<PMemoryBucketProperty> pMemoryBucketProperty = table.getBucketProperty();
        if (!pMemoryBucketProperty.isPresent()) {
            return Optional.empty();
        }

        Map<String, ColumnHandle> map = table.getColumns().stream()
                .collect(Collectors.toMap(ColumnInfo::getName, ColumnInfo::getHandle));

        ImmutableList.Builder<PMemoryColumnHandle> bucketColumns = ImmutableList.builder();
        for (String bucketColumnName : pMemoryBucketProperty.get().getBucketedBy()) {
            ColumnHandle bucketColumnHandle = map.get(bucketColumnName);
            if (bucketColumnHandle == null) {
                throw new IllegalArgumentException("Bucket property not found");
            }
            bucketColumns.add((PMemoryColumnHandle) bucketColumnHandle);
        }

        int bucketCount = pMemoryBucketProperty.get().getBucketCount();
        return Optional.of(new PMemoryBucketHandle(bucketColumns.build(), bucketCount));
    }

    @Override
    public synchronized ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        ImmutableMap.Builder<String, Object> properties = ImmutableMap.builder();

        PMemoryTableHandle handle = (PMemoryTableHandle) tableHandle;
        return tables.get(handle.getId()).getMetadata();
    }

    @Override
    public synchronized List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        return tables.values().stream()
                .filter(table -> schemaName.map(table.getSchemaName()::equals).orElse(true))
                .map(TableInfo::getSchemaTableName)
                .collect(toList());
    }

    @Override
    public synchronized Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        PMemoryTableHandle handle = (PMemoryTableHandle) tableHandle;
        return tables.get(handle.getId())
                .getColumns().stream()
                .collect(toMap(ColumnInfo::getName, ColumnInfo::getHandle));
    }

    @Override
    public synchronized ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        PMemoryTableHandle handle = (PMemoryTableHandle) tableHandle;
        return tables.get(handle.getId())
                .getColumn(columnHandle)
                .getMetadata();
    }

    @Override
    public synchronized Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        return tables.values().stream()
                .filter(table -> prefix.matches(table.getSchemaTableName()))
                .collect(toMap(TableInfo::getSchemaTableName, handle -> handle.getMetadata().getColumns()));
    }

    @Override
    public synchronized void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        PMemoryTableHandle handle = (PMemoryTableHandle) tableHandle;
        TableInfo info = tables.remove(handle.getId());
        if (info != null) {
            tableIds.remove(info.getSchemaTableName());
        }
    }

    @Override
    public synchronized void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle, SchemaTableName newTableName)
    {
        checkSchemaExists(newTableName.getSchemaName());
        checkTableNotExists(newTableName);

        PMemoryTableHandle handle = (PMemoryTableHandle) tableHandle;
        long tableId = handle.getId();

        TableInfo oldInfo = tables.get(tableId);
        tables.put(tableId, new TableInfo(tableId, newTableName.getSchemaName(), newTableName.getTableName(), oldInfo.getColumns(), oldInfo.getBucketProperty(), oldInfo.getDataFragments()));

        tableIds.remove(oldInfo.getSchemaTableName());
        tableIds.put(newTableName, tableId);
    }

    @Override
    public synchronized void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, boolean ignoreExisting)
    {
        ConnectorOutputTableHandle outputTableHandle = beginCreateTable(session, tableMetadata, Optional.empty());
        finishCreateTable(session, outputTableHandle, ImmutableList.of(), ImmutableList.of());
    }

    @Override
    public Optional<ConnectorNewTableLayout> getNewTableLayout(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        //check case where there is no bucketing
        Optional<PMemoryBucketProperty> bucketProperty = PMemoryTableProperties.getBucketProperty(tableMetadata.getProperties());
        if (!bucketProperty.isPresent()) {
            return Optional.empty();
        }

        //there is bucketing, return layout with appropriate handle

        long tableId = nextTableId.get();

        List<String> bucketedBy = bucketProperty.get().getBucketedBy();

        return Optional.of(new ConnectorNewTableLayout(
                new PMemoryPartitioningHandle(
                        tableId,
                        bucketProperty.get().getBucketCount()),
                bucketedBy));
    }

    @Override
    public synchronized PMemoryOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorNewTableLayout> layout)
    {
        System.out.println("Create table!");
        checkSchemaExists(tableMetadata.getTable().getSchemaName());
        checkTableNotExists(tableMetadata.getTable());
        long nextId = nextTableId.getAndIncrement();
        Set<Node> nodes = nodeManager.getRequiredWorkerNodes();
        checkState(!nodes.isEmpty(), "No Memory nodes available");

        long tableId = nextId;

        Optional<PMemoryBucketProperty> bucketProperty = PMemoryTableProperties.getBucketProperty(tableMetadata.getProperties());
        if (bucketProperty.isPresent()) {
            System.out.println(bucketProperty.get().toString());
        }

        ImmutableList.Builder<ColumnInfo> columns = ImmutableList.builder();
        for (int i = 0; i < tableMetadata.getColumns().size(); i++) {
            ColumnMetadata column = tableMetadata.getColumns().get(i);
            columns.add(new ColumnInfo(new PMemoryColumnHandle(i, column.getName(), column.getType()), column.getName(), column.getType()));
        }

        ImmutableList<ColumnInfo> columnList = columns.build();

        tableIds.put(tableMetadata.getTable(), tableId);
        tables.put(tableId, new TableInfo(
                tableId,
                tableMetadata.getTable().getSchemaName(),
                tableMetadata.getTable().getTableName(),
                columnList,
                bucketProperty,
                new HashMap<>()));

        return new PMemoryOutputTableHandle(tableId, ImmutableSet.copyOf(tableIds.values()), columnList.stream().map(column -> (PMemoryColumnHandle) column.getHandle()).collect(Collectors.toList()), columnList.stream().map(column -> column.getType()).collect(Collectors.toList()), bucketProperty);
    }

    private void checkSchemaExists(String schemaName)
    {
        if (!schemas.contains(schemaName)) {
            throw new SchemaNotFoundException(schemaName);
        }
    }

    private void checkTableNotExists(SchemaTableName tableName)
    {
        if (tableIds.containsKey(tableName)) {
            throw new PrestoException(ALREADY_EXISTS, format("Table [%s] already exists", tableName.toString()));
        }
        if (views.containsKey(tableName)) {
            throw new PrestoException(ALREADY_EXISTS, format("View [%s] already exists", tableName.toString()));
        }
    }

    @Override
    public synchronized Optional<ConnectorOutputMetadata> finishCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        requireNonNull(tableHandle, "tableHandle is null");
        PMemoryOutputTableHandle memoryOutputHandle = (PMemoryOutputTableHandle) tableHandle;

        updateRowsOnHosts(memoryOutputHandle.getTable(), fragments);
        return Optional.empty();
    }

    @Override
    public synchronized PMemoryInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        PMemoryTableHandle memoryTableHandle = (PMemoryTableHandle) tableHandle;

        TableInfo table = tables.get(memoryTableHandle.getId());

        return new PMemoryInsertTableHandle(memoryTableHandle.getId(), ImmutableSet.copyOf(tableIds.values()), table.getColumns().stream().map(column -> (PMemoryColumnHandle) column.getHandle()).collect(Collectors.toList()), table.getColumns().stream().map(column -> column.getType()).collect(Collectors.toList()), table.getBucketProperty());
    }

    @Override
    public synchronized Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session, ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        requireNonNull(insertHandle, "insertHandle is null");
        PMemoryInsertTableHandle memoryInsertHandle = (PMemoryInsertTableHandle) insertHandle;

        updateRowsOnHosts(memoryInsertHandle.getTable(), fragments);
        return Optional.empty();
    }

    @Override
    public synchronized void createView(ConnectorSession session, SchemaTableName viewName, ConnectorViewDefinition definition, boolean replace)
    {
        checkSchemaExists(viewName.getSchemaName());
        if (tableIds.containsKey(viewName)) {
            throw new PrestoException(ALREADY_EXISTS, "Table already exists: " + viewName);
        }

        if (replace) {
            views.put(viewName, definition);
        }
        else if (views.putIfAbsent(viewName, definition) != null) {
            throw new PrestoException(ALREADY_EXISTS, "View already exists: " + viewName);
        }
    }

    @Override
    public synchronized void dropView(ConnectorSession session, SchemaTableName viewName)
    {
        if (views.remove(viewName) == null) {
            throw new ViewNotFoundException(viewName);
        }
    }

    @Override
    public synchronized List<SchemaTableName> listViews(ConnectorSession session, Optional<String> schemaName)
    {
        return views.keySet().stream()
                .filter(viewName -> schemaName.map(viewName.getSchemaName()::equals).orElse(true))
                .collect(toImmutableList());
    }

    @Override
    public synchronized Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, Optional<String> schemaName)
    {
        SchemaTablePrefix prefix = schemaName.map(SchemaTablePrefix::new).orElseGet(SchemaTablePrefix::new);
        return ImmutableMap.copyOf(Maps.filterKeys(views, prefix::matches));
    }

    @Override
    public synchronized Optional<ConnectorViewDefinition> getView(ConnectorSession session, SchemaTableName viewName)
    {
        return Optional.ofNullable(views.get(viewName));
    }

    private void updateRowsOnHosts(long tableId, Collection<Slice> fragments)
    {
        TableInfo info = tables.get(tableId);
        checkState(
                info != null,
                "Uninitialized tableId [%s.%s]",
                info.getSchemaName(),
                info.getTableName());

        Map<HostAddress, PMemoryDataFragment> dataFragments = new HashMap<>(info.getDataFragments());
        for (Slice fragment : fragments) {
            PMemoryDataFragment memoryDataFragment = PMemoryDataFragment.fromSlice(fragment);
            dataFragments.merge(memoryDataFragment.getHostAndPort(), memoryDataFragment, PMemoryDataFragment::merge);
        }

        tables.put(tableId, new TableInfo(tableId, info.getSchemaName(), info.getTableName(), info.getColumns(), info.getBucketProperty(), dataFragments));
    }

    @Override
    public boolean usesLegacyTableLayouts()
    {
        return false;
    }

    @Override
    public ConnectorTableProperties getTableProperties(ConnectorSession session, ConnectorTableHandle table)
    {
        PMemoryTableHandle pMemoryTableHandle = (PMemoryTableHandle) table;

        Optional<ConnectorTablePartitioning> tablePartitioning = Optional.empty();
        if (pMemoryTableHandle.getBucketHandle().isPresent()) {
            tablePartitioning = pMemoryTableHandle.getBucketHandle().map(bucketing -> new ConnectorTablePartitioning(
                    new PMemoryPartitioningHandle(pMemoryTableHandle.getId(), bucketing.getTableBucketCount()),
                    bucketing.getColumns().stream()
                            .map(ColumnHandle.class::cast)
                            .collect(toList())));
        }

        return new ConnectorTableProperties(TupleDomain.all(), tablePartitioning, Optional.empty(), Optional.empty(), ImmutableList.of());
    }

    public List<PMemoryDataFragment> getDataFragments(long tableId)
    {
        return ImmutableList.copyOf(tables.get(tableId).getDataFragments().values());
    }

    @Override
    public Optional<LimitApplicationResult<ConnectorTableHandle>> applyLimit(ConnectorSession session, ConnectorTableHandle handle, long limit)
    {
        PMemoryTableHandle table = (PMemoryTableHandle) handle;

        if (table.getLimit().isPresent() && table.getLimit().getAsLong() <= limit) {
            return Optional.empty();
        }

        return Optional.of(new LimitApplicationResult<>(
                new PMemoryTableHandle(table.getId(), table.getTableName(), OptionalLong.of(limit), OptionalDouble.empty(), table.getPredicate(), Optional.empty()),
                true));
    }

    @Override
    public Optional<ConnectorTableHandle> applySample(ConnectorSession session, ConnectorTableHandle handle, SampleType sampleType, double sampleRatio)
    {
        PMemoryTableHandle table = (PMemoryTableHandle) handle;

        if ((table.getSampleRatio().isPresent() && table.getSampleRatio().getAsDouble() == sampleRatio) || sampleType != SYSTEM || table.getLimit().isPresent()) {
            return Optional.empty();
        }

        return Optional.of(new PMemoryTableHandle(table.getId(), table.getTableName(), table.getLimit(), OptionalDouble.of(table.getSampleRatio().orElse(1) * sampleRatio), table.getPredicate(), Optional.empty()));
    }
}

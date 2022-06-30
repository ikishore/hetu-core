package io.prestosql.plugin.pmemory;

import io.prestosql.spi.HostAddress;
import io.prestosql.spi.Node;
import io.prestosql.spi.NodeManager;
import io.prestosql.spi.connector.*;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.spi.connector.ConnectorBucketNodeMap.createBucketNodeMap;
import static java.util.Objects.requireNonNull;

public class PMemoryNodePartitioningProvider
    implements ConnectorNodePartitioningProvider{

    private final NodeManager nodeManager;

    private PMemoryMetadata metadataHandle;

    @Inject
    public PMemoryNodePartitioningProvider(NodeManager nodeManager)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
    }

    public void setMetadataHandle(PMemoryMetadata metadataHandle)
    {
        this.metadataHandle = metadataHandle;
    }

    @Override
    public BucketFunction getBucketFunction(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorPartitioningHandle partitioningHandle,
            List<Type> partitionChannelTypes,
            int bucketCount)
    {
        PMemoryPartitioningHandle handle = (PMemoryPartitioningHandle) partitioningHandle;
        return new PMemoryBucketFunction(bucketCount , handle.isForUpdateOrDelete());
    }

    //map buckets to nodes
    @Override
    public ConnectorBucketNodeMap getBucketNodeMap(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorPartitioningHandle partitioningHandle)
    {
        PMemoryPartitioningHandle handle = (PMemoryPartitioningHandle) partitioningHandle;

        long tableId = handle.getTableId();

        List<PMemoryDataFragment> dataFragments = metadataHandle.getDataFragments(tableId);

        //this query is table creation, so get a random allocation
        if (dataFragments.size() == 0) {
            return createBucketNodeMap(handle.getBucketCount());
        }

        //use historical allocation to push computation to corresponding partitions

        Map<Integer, Node> nodeMap = new HashMap<>();

        Node firstNode = null;

        for (PMemoryDataFragment dataFragment : dataFragments) {
            List<Node> matchingNodes = nodeManager
                    .getAllNodes()
                    .stream()
                    .filter(node -> node.getHostAndPort().equals(dataFragment.getHostAndPort()))
                    .collect(Collectors.toList());

            if (matchingNodes.size() != 1) {
                throw new UnsupportedOperationException("multiple nodes with same host and port");
            }

            Node currentNode = matchingNodes.get(0);

            for (Map.Entry<Integer, Long> rowsPerBucket : dataFragment.getRows().entrySet()) {
                if (nodeMap.containsKey(rowsPerBucket.getKey())) {
                    throw new UnsupportedOperationException("bucket appears in multiple nodes");
                }

                if (firstNode == null) firstNode = currentNode;

                nodeMap.put(rowsPerBucket.getKey(), currentNode);
            }
        }

        //add mock node for empty partitions

        List<Node> nodeList = new ArrayList<>();

        for (int i = 0; i < handle.getBucketCount(); i++) {
            if (nodeMap.containsKey(i)) {
                nodeList.add(nodeMap.get(i));
            } else {
                nodeList.add(firstNode);
            }
        }

        return createBucketNodeMap(nodeList);
    }

    @Override
    public ToIntFunction<ConnectorSplit> getSplitBucketFunction(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorPartitioningHandle partitioningHandle)
    {
        return value -> ((PMemorySplitWrapper) value).getBucketNumber()
                .orElseThrow(() -> new IllegalArgumentException("Bucket number not set in split"));
    }

    @Override
    public List<ConnectorPartitionHandle> listPartitionHandles(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorPartitioningHandle partitioningHandle)
    {
        PMemoryPartitioningHandle handle = (PMemoryPartitioningHandle) partitioningHandle;
        int bucketCount = handle.getBucketCount();
        return IntStream.range(0, bucketCount).mapToObj(PMemoryPartitionHandle::new).collect(toImmutableList());
    }
}

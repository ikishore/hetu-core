package io.prestosql.plugin.pmemory;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.prestosql.spi.HostAddress;
import io.prestosql.spi.Node;

import java.net.URI;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Strings.emptyToNull;
import static com.google.common.base.Strings.nullToEmpty;
import static java.util.Objects.requireNonNull;

//internal node type that this module understands, it is used to encode the information communicated between tablewriter and tablefinish
public class PMemoryInternalNode
    implements Node {
    private final String nodeIdentifier;
    private final HostAddress hostAndPort;
    private final String version;
    private final boolean coordinator;

    @JsonCreator
    public PMemoryInternalNode(
            @JsonProperty("nodeIdentifier") String nodeIdentifier,
            @JsonProperty("hostAndPort") HostAddress hostAndPort,
            @JsonProperty("version") String version,
            @JsonProperty("coordinator") boolean coordinator)
    {
        nodeIdentifier = emptyToNull(nullToEmpty(nodeIdentifier).trim());
        this.nodeIdentifier = requireNonNull(nodeIdentifier, "nodeIdentifier is null or empty");
        this.hostAndPort = requireNonNull(hostAndPort, "hostAndPort is null");
        this.version = requireNonNull(version, "nodeVersion is null");
        this.coordinator = coordinator;
    }

    @Override
    @JsonProperty
    public String getNodeIdentifier()
    {
        return nodeIdentifier;
    }

    @Override
    public String getHost()
    {
        return hostAndPort.getHostText();
    }

    @Override
    public URI getHttpUri()
    {
        throw new UnsupportedOperationException("URI not supported");
    }

    @Override
    @JsonProperty
    public HostAddress getHostAndPort()
    {
        return hostAndPort;
    }

    @Override
    @JsonProperty
    public String getVersion()
    {
        return version;
    }

    @Override
    @JsonProperty
    public boolean isCoordinator()
    {
        return coordinator;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        PMemoryInternalNode o = (PMemoryInternalNode) obj;
        return nodeIdentifier.equals(o.nodeIdentifier);
    }

    @Override
    public int hashCode()
    {
        return nodeIdentifier.hashCode();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("nodeIdentifier", nodeIdentifier)
                .add("hostAddress", hostAndPort)
                .add("nodeVersion", version)
                .toString();
    }
}

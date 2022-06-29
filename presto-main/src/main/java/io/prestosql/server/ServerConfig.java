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
package io.prestosql.server;

import com.google.common.collect.ImmutableSet;
import io.airlift.configuration.Config;
import io.airlift.units.Duration;
import io.airlift.units.MaxDuration;
import io.airlift.units.MinDuration;
import org.apache.commons.lang3.StringUtils;

import javax.validation.constraints.NotNull;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

public class ServerConfig
{
    private static final String DELIMITER = ",";

    private boolean coordinator = true;
    private String prestoVersion = getClass().getPackage().getImplementationVersion();
    private boolean includeExceptionInResponse = true;
    private Duration gracePeriod = new Duration(2, MINUTES);
    private boolean enhancedErrorReporting = true;
    private Duration httpClientIdleTimeout = new Duration(300, SECONDS);
    private Duration httpClientRequestTimeout = new Duration(1000, SECONDS);
    // Main coordinator TODO: remove this when main coordinator election is implemented

    private final Set<String> admins = new HashSet<>();

    public boolean isAdmin(String user)
    {
        return admins.contains(user);
    }

    public Set<String> getAdmins()
    {
        return ImmutableSet.copyOf(admins);
    }

    @Config("openlookeng.admins")
    public ServerConfig setAdmins(String adminsString)
    {
        if (StringUtils.isEmpty(adminsString)) {
            return this;
        }
        String[] adminsSplit = adminsString.split(DELIMITER);
        Arrays.stream(adminsSplit).forEach(admin -> admins.add(admin.trim()));
        return this;
    }

    public boolean isCoordinator()
    {
        return coordinator;
    }

    @Config("coordinator")
    public ServerConfig setCoordinator(boolean coordinator)
    {
        this.coordinator = coordinator;
        return this;
    }

    @NotNull(message = "presto.version must be provided when it cannot be automatically determined")
    public String getPrestoVersion()
    {
        return prestoVersion;
    }

    @Config("presto.version")
    public ServerConfig setPrestoVersion(String prestoVersion)
    {
        this.prestoVersion = prestoVersion;
        return this;
    }

    public boolean isIncludeExceptionInResponse()
    {
        return includeExceptionInResponse;
    }

    @Config("http.include-exception-in-response")
    public ServerConfig setIncludeExceptionInResponse(boolean includeExceptionInResponse)
    {
        this.includeExceptionInResponse = includeExceptionInResponse;
        return this;
    }

    @MinDuration("0ms")
    @MaxDuration("1h")
    public Duration getGracePeriod()
    {
        return gracePeriod;
    }

    @Config("shutdown.grace-period")
    public ServerConfig setGracePeriod(Duration gracePeriod)
    {
        this.gracePeriod = gracePeriod;
        return this;
    }

    public boolean isEnhancedErrorReporting()
    {
        return enhancedErrorReporting;
    }

    // TODO: temporary kill switch until we're confident the new error handling logic is
    // solid. Placed here for convenience and to avoid creating a new set of throwaway config objects
    // and because the parser is instantiated in the module that wires up the server (ServerMainModule)
    @Config("sql.parser.enhanced-error-reporting")
    public ServerConfig setEnhancedErrorReporting(boolean value)
    {
        this.enhancedErrorReporting = value;
        return this;
    }

    @Config("http.client.idle-timeout")
    public ServerConfig setHttpClientIdleTimeout(Duration httpClientIdleTimeout)
    {
        this.httpClientIdleTimeout = httpClientIdleTimeout;
        return this;
    }

    @MinDuration("30s")
    public Duration getHttpClientIdleTimeout()
    {
        return httpClientIdleTimeout;
    }

    @Config("http.client.request-timeout")
    public ServerConfig setHttpClientRequestTimeout(Duration httpClientRequestTimeout)
    {
        this.httpClientRequestTimeout = httpClientRequestTimeout;
        return this;
    }

    @MinDuration("10s")
    public Duration getHttpClientRequestTimeout()
    {
        return httpClientRequestTimeout;
    }
}

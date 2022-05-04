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
package io.prestosql.failuredetector;

import com.google.common.base.Ticker;
import io.prestosql.server.remotetask.Backoff;
import io.prestosql.spi.HostAddress;

public class TimeoutFailureRetryPolicy
                extends AbstractFailureRetryPolicy
{
    private TimeoutFailureRetryConfig config;

    public TimeoutFailureRetryPolicy(TimeoutFailureRetryConfig config)
    {
        super(new Backoff(config.getMaxTimeoutDuration()));
        this.config = config;
    }

    public TimeoutFailureRetryPolicy(TimeoutFailureRetryConfig config, Ticker ticker)
    {
        super(new Backoff(config.getMaxTimeoutDuration(), ticker));
        this.config = config;
    }

    @Override
    public boolean hasFailed(HostAddress address)
    {
        return getBackoff().failure();
    }
}

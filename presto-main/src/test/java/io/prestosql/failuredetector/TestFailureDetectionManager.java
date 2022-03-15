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

import io.prestosql.spi.failuredetector.FailureRetryPolicy;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Properties;

import static org.testng.Assert.assertEquals;

public class TestFailureDetectionManager
{
    @Test
    public void testDefaultRetryProfile()
    {
        FailureRetryConfig cfg = new FailureRetryConfig();
        cfg.setFailureRetryPolicyProfile("test");
        Properties prop = new Properties();
        prop.setProperty(FailureRetryPolicy.FD_RETRY_TYPE, cfg.getFailureRetryPolicyProfile());
        prop.setProperty(FailureRetryPolicy.MAX_RETRY_COUNT, "10");
        prop.setProperty(FailureRetryPolicy.MAX_TIMEOUT_DURATION, "60s");
        FailureDetectorManager.addFrConfigs(cfg.getFailureRetryPolicyProfile(), prop);

        assertEquals(FailureDetectorManager.getAvailableFrConfigs().size(), 1);

        FailureDetectorManager.addFailureRetryFactory(new MaxRetryFailureRetryFactory());

        FailureDetectorManager failureDetectorManager = new FailureDetectorManager(cfg, new NoOpFailureDetector());
        assertEquals(FailureDetectorManager.getAvailableFrConfigs().size(), 1);

        Map<String, Properties> cfgProp = FailureDetectorManager.getAvailableFrConfigs();
        assertEquals(cfgProp.size(), 1);

        Properties checkprop = cfgProp.get("test");
        assertEquals("10", checkprop.getProperty(FailureRetryPolicy.MAX_RETRY_COUNT));
        assertEquals("60s", checkprop.getProperty(FailureRetryPolicy.MAX_TIMEOUT_DURATION));
    }

    @Test
    public void testDefaultRetryProfile1()
    {
        FailureDetectorManager.removeallFrConfigs();
        FailureRetryConfig cfg = new FailureRetryConfig();
        cfg.setFailureRetryPolicyProfile("default");
        Properties prop = new Properties();
        prop.setProperty(FailureRetryPolicy.FD_RETRY_TYPE, cfg.getFailureRetryPolicyProfile());
        prop.setProperty(FailureRetryPolicy.MAX_RETRY_COUNT, "100");
        prop.setProperty(FailureRetryPolicy.MAX_TIMEOUT_DURATION, "34560s");
        FailureDetectorManager.addFrConfigs(cfg.getFailureRetryPolicyProfile(), prop);

        assertEquals(FailureDetectorManager.getAvailableFrConfigs().size(), 1);

        FailureDetectorManager.addFailureRetryFactory(new MaxRetryFailureRetryFactory());

        FailureDetectorManager failureDetectorManager = new FailureDetectorManager(cfg, new NoOpFailureDetector());
        assertEquals(FailureDetectorManager.getAvailableFrConfigs().size(), 1);

        Map<String, Properties> cfgProp = FailureDetectorManager.getAvailableFrConfigs();
        assertEquals(cfgProp.size(), 1);

        Properties checkprop = cfgProp.get("default");
        assertEquals("100", checkprop.getProperty(FailureRetryPolicy.MAX_RETRY_COUNT));
        assertEquals("34560s", checkprop.getProperty(FailureRetryPolicy.MAX_TIMEOUT_DURATION));
    }

    @Test
    public void testDefaultRetryProfile2()
    {
        FailureDetectorManager.removeallFrConfigs();
        FailureDetectorManager failureDetectorManager = new FailureDetectorManager(new NoOpFailureDetector(), "30s");
        assertEquals(FailureDetectorManager.getAvailableFrConfigs().size(), 1);

        Map<String, Properties> cfgProp = FailureDetectorManager.getAvailableFrConfigs();
        assertEquals(cfgProp.size(), 1);

        Properties checkprop = cfgProp.get("default");
        assertEquals(FailureRetryPolicy.TIMEOUT, checkprop.getProperty(FailureRetryPolicy.FD_RETRY_TYPE));
        assertEquals("30s", checkprop.getProperty(FailureRetryPolicy.MAX_TIMEOUT_DURATION));
    }
}

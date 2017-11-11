/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ml.littlebulb.presto.kudu;

import com.google.common.base.Splitter;
import io.airlift.configuration.Config;
import io.airlift.units.Duration;
import io.airlift.units.MaxDuration;
import io.airlift.units.MinDuration;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Configuration read from etc/catalog/kudu.properties
 */
public class KuduClientConfig {
    private static final Splitter SPLITTER = Splitter.on(',').trimResults().omitEmptyStrings();

    private List<String> masterAddresses;
    private Duration defaultAdminOperationTimeout = new Duration(30, TimeUnit.SECONDS);
    private Duration defaultOperationTimeout = new Duration(30, TimeUnit.SECONDS);
    private Duration defaultSocketReadTimeout = new Duration(10, TimeUnit.SECONDS);
    private boolean disableStatistics = false;
    private String tenant = null;

    @NotNull
    @Size(min = 1)
    public List<String> getMasterAddresses()
    {
        return masterAddresses;
    }

    @Config("kudu.client.master-addresses")
    public KuduClientConfig setMasterAddresses(String commaSeparatedList)
    {
        this.masterAddresses = SPLITTER.splitToList(commaSeparatedList);
        return this;
    }

    public KuduClientConfig setMasterAddresses(String... contactPoints)
    {
        this.masterAddresses = Arrays.asList(contactPoints);
        return this;
    }

    @Config("kudu.client.defaultAdminOperationTimeout")
    public KuduClientConfig setDefaultAdminOperationTimeout(Duration timeout)
    {
        this.defaultAdminOperationTimeout = timeout;
        return this;
    }

    @MinDuration("1s")
    @MaxDuration("1h")
    public Duration getDefaultAdminOperationTimeout()
    {
        return defaultAdminOperationTimeout;
    }

    @Config("kudu.client.defaultOperationTimeout")
    public KuduClientConfig setDefaultOperationTimeout(Duration timeout)
    {
        this.defaultOperationTimeout = timeout;
        return this;
    }

    @MinDuration("1s")
    @MaxDuration("1h")
    public Duration getDefaultOperationTimeout()
    {
        return defaultOperationTimeout;
    }

    @Config("kudu.client.defaultSocketReadTimeout")
    public KuduClientConfig setDefaultSocketReadTimeout(Duration timeout)
    {
        this.defaultSocketReadTimeout = timeout;
        return this;
    }

    @MinDuration("1s")
    @MaxDuration("1h")
    public Duration getDefaultSocketReadTimeout()
    {
        return defaultSocketReadTimeout;
    }

    public boolean isDisableStatistics()
    {
        return this.disableStatistics;
    }

    @Config("kudu.client.disableStatistics")
    public KuduClientConfig setDisableStatistics(boolean disableStatistics)
    {
        this.disableStatistics = disableStatistics;
        return this;
    }

    public String getTenant()
    {
        return tenant;
    }

    @Config("kudu.session.tenant")
    public KuduClientConfig setTenant(String tenant)
    {
        this.tenant = tenant;
        return this;
    }
}

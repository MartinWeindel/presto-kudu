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

import com.facebook.presto.spi.connector.*;
import com.facebook.presto.spi.procedure.Procedure;
import com.facebook.presto.spi.type.TypeManager;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.multibindings.ProvidesIntoSet;
import ml.littlebulb.presto.kudu.procedures.RangePartitionProcedures;
import ml.littlebulb.presto.kudu.properties.KuduTableProperties;
import org.apache.kudu.client.KuduClient;

import javax.inject.Singleton;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;

public class KuduModule implements Module {

    private final String connectorId;
    private final TypeManager typeManager;

    public KuduModule(String connectorId, TypeManager typeManager) {
        this.connectorId = requireNonNull(connectorId, "connector id is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    public void configure(Binder binder) {
        binder.bind(TypeManager.class).toInstance(typeManager);

        binder.bind(KuduConnector.class).in(Scopes.SINGLETON);
        binder.bind(KuduConnectorId.class).toInstance(new KuduConnectorId(connectorId));
        binder.bind(KuduMetadata.class).in(Scopes.SINGLETON);
        binder.bind(KuduTableProperties.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorSplitManager.class).to(KuduSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorRecordSetProvider.class).to(KuduRecordSetProvider.class)
                .in(Scopes.SINGLETON);
        binder.bind(ConnectorPageSourceProvider.class).to(KuduPageSourceProvider.class)
                .in(Scopes.SINGLETON);
        binder.bind(ConnectorPageSinkProvider.class).to(KuduPageSinkProvider.class).in(Scopes.SINGLETON);
        binder.bind(KuduHandleResolver.class).in(Scopes.SINGLETON);
        binder.bind(KuduRecordSetProvider.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(KuduClientConfig.class);

        Multibinder.newSetBinder(binder, Procedure.class);

        binder.bind(RangePartitionProcedures.class).in(Scopes.SINGLETON);
    }

    @ProvidesIntoSet
    public static Procedure getCreateRangePartitionProcedure(RangePartitionProcedures procedures)
    {
        return procedures.getCreatePartitionProcedure();
    }
/*
    @ProvidesIntoSet
    public static Procedure getDeleteRangePartitionProcedure(RangePartitionProcedures procedures)
    {
        return procedure.getProcedure();
    }

    @ProvidesIntoSet
    public static Procedure getListRangePartitionsProcedure(RangePartitionProcedures procedures)
    {
        return procedure.getProcedure();
    }
*/
    @Singleton
    @Provides
    public static KuduClientSession createKuduClientSession(
            KuduConnectorId connectorId,
            KuduClientConfig config) {
        requireNonNull(config, "config is null");

        KuduClient.KuduClientBuilder builder = new KuduClient.KuduClientBuilder(config.getMasterAddresses());
        builder.defaultAdminOperationTimeoutMs(config.getDefaultAdminOperationTimeout().toMillis());
        builder.defaultOperationTimeoutMs(config.getDefaultOperationTimeout().toMillis());
        builder.defaultSocketReadTimeoutMs(config.getDefaultSocketReadTimeout().toMillis());
        if (config.isDisableStatistics()) {
            builder.disableStatistics();
        }
        KuduClient client = builder.build();
        String tenant = config.getTenant();
        return new NativeKuduClientSession(connectorId, client, tenant);
    }
}

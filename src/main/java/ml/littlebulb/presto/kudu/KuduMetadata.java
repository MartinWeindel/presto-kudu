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

import com.facebook.presto.spi.*;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorOutputMetadata;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarbinaryType;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import ml.littlebulb.presto.kudu.properties.KuduTableProperties;
import io.airlift.slice.Slice;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.client.KuduTable;

import javax.inject.Inject;
import java.util.*;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static ml.littlebulb.presto.kudu.Types.checkType;
import static java.util.Objects.requireNonNull;

public class KuduMetadata implements ConnectorMetadata {
    private final String connectorId;
    private KuduClientSession clientSession;

    @Inject
    public KuduMetadata(KuduConnectorId connectorId, KuduClientSession clientSession) {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.clientSession = requireNonNull(clientSession, "clientSession is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session) {
        return clientSession.listSchemaNames();
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull) {
        return clientSession.listTables(schemaNameOrNull);
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session,
                                                                       SchemaTablePrefix prefix) {
        requireNonNull(prefix, "SchemaTablePrefix is null");

        List<SchemaTableName> tables;
        if (prefix.getSchemaName() == null) {
            tables = listTables(session, null);
        } else if (prefix.getTableName() == null) {
            tables = listTables(session, prefix.getSchemaName());
        } else {
            tables = ImmutableList.of(new SchemaTableName(prefix.getSchemaName(), prefix.getTableName()));
        }

        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName tableName : tables) {
            KuduTableHandle tableHandle = getTableHandle(session, tableName);
            ConnectorTableMetadata tableMetadata = getTableMetadata(tableHandle);
            columns.put(tableName, tableMetadata.getColumns());
        }
        return columns.build();
    }


    private ConnectorTableMetadata getTableMetadata(KuduTableHandle tableHandle) {
        KuduTable table = tableHandle.getTable(clientSession);
        Schema schema = table.getSchema();

        List<ColumnMetadata> columnsMetaList = schema.getColumns().stream()
                .filter(col -> !col.isKey() || !col.getName().equals(KuduColumnHandle.ROW_ID))
                .map(col -> {
                    KuduType kuduType = KuduType.fromKuduClientType(col.getType());
                    StringBuilder extra = new StringBuilder();
                    if (col.isKey()) {
                        extra.append("key, ");
                    } else if (col.isNullable()) {
                        extra.append("nullable, ");
                    }
                    if (col.getEncoding() != null) {
                        extra.append("encoding=").append(col.getEncoding().name()).append(", ");
                    }
                    if (col.getCompressionAlgorithm() != null) {
                        extra.append("compression=").append(col.getCompressionAlgorithm().name()).append(", ");
                    }
                    if (extra.length() > 2) {
                        extra.setLength(extra.length() - 2);
                    }
                    return new ColumnMetadata(col.getName(), kuduType.getPrestoType(), null, extra.toString(), false);
                }).collect(toImmutableList());

        Map<String, Object> properties = clientSession.getTableProperties(tableHandle);
        return new ConnectorTableMetadata(tableHandle.getSchemaTableName(), columnsMetaList, properties);
    }

    private KuduTableHandle fromConnectorTableHandle(ConnectorSession session, ConnectorTableHandle tableHandle) {
        return checkType(tableHandle, KuduTableHandle.class, "tableHandle");
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session,
                                                      ConnectorTableHandle connectorTableHandle) {
        KuduTableHandle tableHandle = fromConnectorTableHandle(session, connectorTableHandle);
        Schema schema = clientSession.getTableSchema(tableHandle);

        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        for (int i = 0; i < schema.getColumnCount(); i++) {
            ColumnSchema col = schema.getColumnByIndex(i);
            String name = col.getName();
            KuduType type = KuduType.fromKuduClientType(col.getType());
            KuduColumnHandle columnHandle = new KuduColumnHandle(name, i, type);
            columnHandles.put(name, columnHandle);
        }

        return columnHandles.build();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session,
                                            ConnectorTableHandle tableHandle, ColumnHandle columnHandle) {
        fromConnectorTableHandle(session, tableHandle);
        KuduColumnHandle kuduColumnHandle = checkType(columnHandle, KuduColumnHandle.class, "columnHandle");
        if (kuduColumnHandle.isVirtualRowId()) {
            return new ColumnMetadata(KuduColumnHandle.ROW_ID, VarbinaryType.VARBINARY, null, true);
        } else {
            return kuduColumnHandle.getColumnMetadata();
        }
    }

    @Override
    public KuduTableHandle getTableHandle(ConnectorSession session, SchemaTableName schemaTableName) {
        try {
            KuduTable table = clientSession.openTable(schemaTableName);
            return new KuduTableHandle(connectorId, schemaTableName, table);
        } catch (Exception e) {
            return null;
        }
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session,
                                                            ConnectorTableHandle tableHandle, Constraint<ColumnHandle> constraint,
                                                            Optional<Set<ColumnHandle>> desiredColumns) {
        KuduTableHandle handle = fromConnectorTableHandle(session, tableHandle);
        ConnectorTableLayout layout = new ConnectorTableLayout(
                new KuduTableLayoutHandle(handle, constraint.getSummary(), desiredColumns));
        return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle) {
        return new ConnectorTableLayout(handle);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle) {
        return getTableMetadataInternal(session, tableHandle);
    }

    private ConnectorTableMetadata getTableMetadataInternal(ConnectorSession session, ConnectorTableHandle tableHandle) {
        KuduTableHandle kuduTableHandle = fromConnectorTableHandle(session, tableHandle);
        return getTableMetadata(kuduTableHandle);
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName, Map<String, Object> properties) {
        clientSession.createSchema(schemaName);
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName) {
        clientSession.dropSchema(schemaName);
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, boolean ignoreExisting) {
        clientSession.createTable(tableMetadata, ignoreExisting);
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle) {
        KuduTableHandle kuduTableHandle = fromConnectorTableHandle(session, tableHandle);
        clientSession.dropTable(kuduTableHandle.getSchemaTableName());
    }

    @Override
    public void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle, SchemaTableName newTableName) {
        KuduTableHandle kuduTableHandle = fromConnectorTableHandle(session, tableHandle);
        clientSession.renameTable(kuduTableHandle.getSchemaTableName(), newTableName);
    }

    @Override
    public void addColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnMetadata column) {
        KuduTableHandle kuduTableHandle = fromConnectorTableHandle(session, tableHandle);
        clientSession.addColumn(kuduTableHandle.getSchemaTableName(), column);
    }

    @Override
    public void dropColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column) {
        KuduTableHandle kuduTableHandle = fromConnectorTableHandle(session, tableHandle);
        KuduColumnHandle kuduColumnHandle = (KuduColumnHandle) column;
        clientSession.dropColumn(kuduTableHandle.getSchemaTableName(), kuduColumnHandle.getName());
    }

    @Override
    public void renameColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle source, String target) {
        KuduTableHandle kuduTableHandle = fromConnectorTableHandle(session, tableHandle);
        KuduColumnHandle kuduColumnHandle = (KuduColumnHandle) source;
        clientSession.renameColumn(kuduTableHandle.getSchemaTableName(), kuduColumnHandle.getName(), target);
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle connectorTableHandle) {
        KuduTableHandle tableHandle = fromConnectorTableHandle(session, connectorTableHandle);

        KuduTable table = tableHandle.getTable(clientSession);
        Schema schema = table.getSchema();

        List<ColumnSchema> columns = schema.getColumns();
        List<String> columnNames = columns.stream().map(ColumnSchema::getName).collect(toImmutableList());
        List<Type> columnTypes = columns.stream()
                .map(column -> KuduType.fromKuduClientType(column.getType()).getPrestoType()).collect(toImmutableList());

        return new KuduInsertTableHandle(
                connectorId,
                tableHandle.getSchemaTableName(),
                columnNames,
                columnTypes,
                table);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session,
                                                          ConnectorInsertTableHandle insertHandle,
                                                          Collection<Slice> fragments) {
        return Optional.empty();
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata,
                                                       Optional<ConnectorNewTableLayout> layout) {
        boolean generateUUID = !tableMetadata.getProperties().containsKey(KuduTableProperties.PARTITION_DESIGN);
        ConnectorTableMetadata finalTableMetadata = tableMetadata;
        if (generateUUID) {
            String rowId = KuduColumnHandle.ROW_ID;
            List<ColumnMetadata> copy = new ArrayList<>(tableMetadata.getColumns());
            copy.add(0, new ColumnMetadata(rowId, VarcharType.VARCHAR, "key=true", null, true));
            List<ColumnMetadata> finalColumns = ImmutableList.copyOf(copy);
            Map<String, Object> propsCopy = new HashMap<>(tableMetadata.getProperties());
            propsCopy.put(KuduTableProperties.COLUMN_DESIGN, "{\"" + rowId + "\": {\"key\": true}}");
            propsCopy.put(KuduTableProperties.PARTITION_DESIGN, "{\"hash\": [{\"columns\": [\"" + rowId + "\"], \"buckets\": 2}]}");
            propsCopy.put(KuduTableProperties.NUM_REPLICAS, 1);
            Map<String, Object> finalProperties = ImmutableMap.copyOf(propsCopy);
            finalTableMetadata = new ConnectorTableMetadata(tableMetadata.getTable(),
                    finalColumns, finalProperties, tableMetadata.getComment());
        }
        KuduTable table = clientSession.createTable(finalTableMetadata, false);

        Schema schema = table.getSchema();

        List<ColumnSchema> columns = schema.getColumns();
        List<String> columnNames = columns.stream().map(ColumnSchema::getName).collect(toImmutableList());
        List<Type> columnTypes = columns.stream()
                .map(column -> KuduType.fromKuduClientType(column.getType()).getPrestoType()).collect(toImmutableList());
        List<Type> columnOriginalTypes = finalTableMetadata.getColumns().stream()
                .map(ColumnMetadata::getType).collect(toImmutableList());

        return new KuduOutputTableHandle(
                connectorId,
                finalTableMetadata.getTable(),
                columnOriginalTypes,
                columnNames,
                columnTypes,
                generateUUID,
                table);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(ConnectorSession session,
                                                               ConnectorOutputTableHandle tableHandle,
                                                               Collection<Slice> fragments) {
        return Optional.empty();
    }

    @Override
    public ColumnHandle getUpdateRowIdColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle) {
        return KuduColumnHandle.ROW_ID_HANDLE;
    }

    @Override
    public ConnectorTableHandle beginDelete(ConnectorSession session, ConnectorTableHandle tableHandle) {
        KuduTableHandle kuduTableHandle = fromConnectorTableHandle(session, tableHandle);
        return kuduTableHandle;
    }

    @Override
    public void finishDelete(ConnectorSession session, ConnectorTableHandle tableHandle, Collection<Slice> fragments) {
    }

    @Override
    public boolean supportsMetadataDelete(ConnectorSession session, ConnectorTableHandle tableHandle, ConnectorTableLayoutHandle tableLayoutHandle) {
        return false;
    }

    @Override
    public TableIdentity getTableIdentity(ConnectorTableHandle tableHandle) {
        KuduTableHandle kuduTableHandle = checkType(tableHandle, KuduTableHandle.class, "tableHandle");
        String tableId = kuduTableHandle.getTable(clientSession).getTableId();
        return new KuduTableIdentity(tableId);
    }

    @Override
    public TableIdentity deserializeTableIdentity(byte[] bytes) {
        return KuduTableIdentity.deserialize(bytes);
    }

}
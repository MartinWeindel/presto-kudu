package ml.littlebulb.presto.kudu;

import com.facebook.presto.spi.*;
import com.facebook.presto.spi.predicate.*;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import ml.littlebulb.presto.kudu.properties.*;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.google.common.collect.ImmutableList.toImmutableList;


public class NativeKuduClientSession implements KuduClientSession {
    public static final String NULL_SCHEMA = "null_schema";
    private final Logger log = Logger.get(getClass());
    private final KuduConnectorId connectorId;
    private final String tenantPrefix;
    private final String rawSchemasTableName;
    private final KuduClient client;
    private KuduTable rawSchemasTable;

    public NativeKuduClientSession(KuduConnectorId connectorId, KuduClient client, String tenant) {
        this.connectorId = connectorId;
        this.client = client;
        this.tenantPrefix = tenant == null ? "" : tenant + ".";
        this.rawSchemasTableName = "$schemas";
    }

    @Override
    public List<String> listSchemaNames() {
        try {
            if (rawSchemasTable == null) {
                if (!client.tableExists(rawSchemasTableName)) {
                    createAndFillSchemasTable();
                }
                rawSchemasTable = getSchemasTable();
            }

            ColumnSchema tenantColumn = rawSchemasTable.getSchema().getColumnByIndex(0);
            KuduScanner scanner = client.newScannerBuilder(rawSchemasTable)
                    .addPredicate(KuduPredicate.newComparisonPredicate(tenantColumn, KuduPredicate.ComparisonOp.EQUAL, tenantPrefix))
                    .setProjectedColumnIndexes(ImmutableList.of(1))
                    .build();
            RowResultIterator iterator = scanner.nextRows();
            ArrayList<String> result = new ArrayList<>();
            while (iterator != null) {
                for (RowResult row : iterator) {
                    result.add(row.getString(0));
                }
                iterator = scanner.nextRows();
            }
            return result;
        } catch (KuduException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, e);
        }
    }

    private KuduTable getSchemasTable() throws KuduException {
        if (rawSchemasTable == null) {
            rawSchemasTable = client.openTable(rawSchemasTableName);
        }
        return rawSchemasTable;
    }

    private void createAndFillSchemasTable() throws KuduException {
        List<String> existingSchemaNames = listSchemaNamesFromTablets();
        ColumnSchema tenantColumnSchema = new ColumnSchema.ColumnSchemaBuilder("tenant", Type.STRING)
                .key(true).build();
        ColumnSchema schemaColumnSchema = new ColumnSchema.ColumnSchemaBuilder("schema", Type.STRING)
                .key(true).build();
        Schema schema = new Schema(ImmutableList.of(tenantColumnSchema, schemaColumnSchema));
        CreateTableOptions options = new CreateTableOptions();
        options.setNumReplicas(1); // TODO config
        options.addHashPartitions(ImmutableList.of(tenantColumnSchema.getName()), 2);
        KuduTable schemasTable = client.createTable(rawSchemasTableName, schema, options);
        KuduSession session = client.newSession();
        session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND);
        try {
            for (String schemaName : existingSchemaNames) {
                Insert insert = schemasTable.newInsert();
                fillSchemaRow(insert.getRow(), schemaName);
                session.apply(insert);
            }
        } finally {
            session.close();
        }
    }

    private List<String> listSchemaNamesFromTablets() {
        final String prefix = tenantPrefix;
        List<String> tables = internalListTables(prefix);
        LinkedHashSet<String> schemas = new LinkedHashSet<>();
        for (String table : tables) {
            int index = table.indexOf('.', prefix.length());
            if (index > prefix.length()) {
                String schema = table.substring(prefix.length(), index);
                schemas.add(schema);
            } else {
                schemas.add(NULL_SCHEMA);
            }
        }
        return ImmutableList.copyOf(schemas);
    }

    private List<String> internalListTables(String prefix) {
        try {
            List<String> tables;
            if (prefix.isEmpty()) {
                tables = client.getTablesList().getTablesList();
            } else {
                tables = client.getTablesList(prefix).getTablesList().stream().
                        filter(name -> name.startsWith(prefix)).collect(toImmutableList());
            }
            return tables;
        } catch (KuduException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, e);
        }
    }

    @Override
    public List<SchemaTableName> listTables(String schemaNameOrNull) {
        final int offset = tenantPrefix.length();
        final String prefix;
        if (schemaNameOrNull == null) {
            prefix = tenantPrefix;
        } else {
            prefix = tenantPrefix + schemaNameOrNull + ".";
        }
        List<String> tables = internalListTables(prefix);
        return tables.stream().map(name -> {
            int index = name.indexOf('.', offset);
            if (index > offset) {
                String schema = name.substring(offset, index);
                String table = name.substring(index + 1);
                return new SchemaTableName(schema, table);
            } else {
                String schema = NULL_SCHEMA;
                String table = name.substring(offset);
                return new SchemaTableName(schema, table);
            }
        }).collect(toImmutableList());
    }


    @Override
    public boolean tableExists(SchemaTableName schemaTableName) {
        String rawName = toRawName(schemaTableName);
        try {
            return client.tableExists(rawName);
        } catch (KuduException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, e);
        }
    }

    @Override
    public Schema getTableSchema(KuduTableHandle tableHandle) {
        KuduTable table = tableHandle.getTable(this);
        return table.getSchema();
    }

    @Override
    public Map<String, Object> getTableProperties(KuduTableHandle tableHandle) {
        KuduTable table = tableHandle.getTable(this);
        return KuduTableProperties.toMap(table);
    }


    @Override
    public List<KuduSplit> buildKuduSplits(KuduTableLayoutHandle layoutHandle) {
        KuduTableHandle tableHandle = layoutHandle.getTableHandle();
        KuduTable table = tableHandle.getTable(this);
        final int primaryKeyColumnCount = table.getSchema().getPrimaryKeyColumnCount();
        KuduScanToken.KuduScanTokenBuilder builder = client.newScanTokenBuilder(table);

        TupleDomain<ColumnHandle> constraintSummary = layoutHandle.getConstraintSummary();
        if (!addConstraintPredicates(table, builder, constraintSummary)) {
            return ImmutableList.of();
        }

        Optional<Set<ColumnHandle>> desiredColumns = layoutHandle.getDesiredColumns();
        if (desiredColumns.isPresent()) {
            if (desiredColumns.get().contains(KuduColumnHandle.ROW_ID_HANDLE)) {
                List<Integer> columnIndexes = IntStream
                        .range(0, primaryKeyColumnCount)
                        .boxed().collect(Collectors.toList());
                for (ColumnHandle columnHandle : desiredColumns.get()) {
                    if (columnHandle instanceof KuduColumnHandle) {
                        KuduColumnHandle k = (KuduColumnHandle) columnHandle;
                        int index = k.getOrdinalPosition();
                        if (index >= primaryKeyColumnCount) {
                            columnIndexes.add(index);
                        }
                    }
                }
                builder.setProjectedColumnIndexes(columnIndexes);
            } else {
                List<Integer> columnIndexes = desiredColumns.get().stream()
                        .map(handle -> ((KuduColumnHandle) handle).getOrdinalPosition())
                        .collect(toImmutableList());
                builder.setProjectedColumnIndexes(columnIndexes);
            }
        }

        List<KuduScanToken> tokens = builder.build();
        return tokens.stream()
                .map(token -> toKuduSplit(tableHandle, token, primaryKeyColumnCount))
                .collect(toImmutableList());
    }

    @Override
    public KuduScanner createScanner(KuduSplit kuduSplit) {
        try {
            KuduScanner scanner = KuduScanToken.deserializeIntoScanner(kuduSplit.getPb(), client);
            return scanner;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public KuduTable openTable(SchemaTableName schemaTableName) {
        String rawName = toRawName(schemaTableName);
        try {
            KuduTable table = client.openTable(rawName);
            return table;
        } catch (Exception e) {
            log.debug("Error on doOpenTable: " + e, e);
            if (!listSchemaNames().contains(schemaTableName.getSchemaName())) {
                throw new SchemaNotFoundException(schemaTableName.getSchemaName());
            } else {
                throw new TableNotFoundException(schemaTableName);
            }
        }
    }

    @Override
    public KuduSession newSession() {
        return client.newSession();
    }

    @Override
    public void createSchema(String schemaName) {
        try {
            KuduTable schemasTable = getSchemasTable();
            KuduSession session = client.newSession();
            try {
                Upsert upsert = schemasTable.newUpsert();
                fillSchemaRow(upsert.getRow(), schemaName);
                session.apply(upsert);
            } finally {
                session.close();
            }
        } catch (KuduException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, e);
        }
    }

    private void fillSchemaRow(PartialRow row, String schemaName) {
        row.addString(0, tenantPrefix);
        row.addString(1, schemaName);
    }

    @Override
    public void dropSchema(String schemaName) {
        try {
            for (SchemaTableName table: listTables(schemaName)) {
                dropTable(table);
            }
            KuduTable schemasTable = getSchemasTable();
            KuduSession session = client.newSession();
            try {
                Delete delete = schemasTable.newDelete();
                fillSchemaRow(delete.getRow(), schemaName);
                session.apply(delete);
            } finally {
                session.close();
            }
        } catch (KuduException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, e);
        }
    }

    @Override
    public void dropTable(SchemaTableName schemaTableName) {
        try {
            String rawName = toRawName(schemaTableName);
            client.deleteTable(rawName);
        } catch (KuduException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, e);
        }
    }

    @Override
    public void renameTable(SchemaTableName schemaTableName, SchemaTableName newSchemaTableName) {
        try {
            String rawName = toRawName(schemaTableName);
            String newRawName = toRawName(newSchemaTableName);
            AlterTableOptions alterOptions = new AlterTableOptions();
            alterOptions.renameTable(newRawName);
            client.alterTable(rawName, alterOptions);
        } catch (KuduException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, e);
        }
    }

    @Override
    public KuduTable createTable(ConnectorTableMetadata tableMetadata) {
        try {
            String rawName = toRawName(tableMetadata.getTable());
            List<ColumnMetadata> columns = tableMetadata.getColumns();
            Map<String, Object> properties = tableMetadata.getProperties();

            Schema schema = buildSchema(columns, properties);
            CreateTableOptions options = buildCreateTableOptions(properties);
            return client.createTable(rawName, schema, options);
        } catch (KuduException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, e);
        }

    }

    @Override
    public void addColumn(SchemaTableName schemaTableName, ColumnMetadata column) {
        try {
            String rawName = toRawName(schemaTableName);
            AlterTableOptions alterOptions = new AlterTableOptions();
            Type type = KuduType.fromPrestoType(column.getType()).getKuduClientType();
            alterOptions.addNullableColumn(column.getName(), type);
            client.alterTable(rawName, alterOptions);
        } catch (KuduException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, e);
        }
    }

    @Override
    public void dropColumn(SchemaTableName schemaTableName, String name) {
        try {
            String rawName = toRawName(schemaTableName);
            AlterTableOptions alterOptions = new AlterTableOptions();
            alterOptions.dropColumn(name);
            client.alterTable(rawName, alterOptions);
        } catch (KuduException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, e);
        }
    }

    @Override
    public void renameColumn(SchemaTableName schemaTableName, String oldName, String newName) {
        try {
            String rawName = toRawName(schemaTableName);
            AlterTableOptions alterOptions = new AlterTableOptions();
            alterOptions.renameColumn(oldName, newName);
            client.alterTable(rawName, alterOptions);
        } catch (KuduException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, e);
        }
    }

    private Schema buildSchema(List<ColumnMetadata> columns, Map<String, Object> properties) {
        Optional<Map<String, ColumnDesign>> optColumnDesign = KuduTableProperties.getColumnDesign(properties);

        Map<String, ColumnDesign> columnDesignMap = optColumnDesign.orElse(ImmutableMap.of());

        List<ColumnSchema> kuduColumns = columns.stream()
                .map(columnMetadata -> toColumnSchema(columnMetadata, columnDesignMap))
                .collect(ImmutableList.toImmutableList());
        return new Schema(kuduColumns);
    }

    private ColumnSchema toColumnSchema(ColumnMetadata columnMetadata, Map<String, ColumnDesign> columnDesignMap) {
        String name = columnMetadata.getName();
        ColumnDesign design = columnDesignMap.getOrDefault(name, ColumnDesign.DEFAULT);
        KuduType ktype = KuduType.fromPrestoType(columnMetadata.getType());
        ColumnSchema.ColumnSchemaBuilder builder = new ColumnSchema.ColumnSchemaBuilder(name, ktype.getKuduClientType());
        builder.key(design.isKey()).nullable(design.isNullable());
        setEncoding(name, builder, design);
        setCompression(name, builder, design);
        return builder.build();
    }

    private void setCompression(String name, ColumnSchema.ColumnSchemaBuilder builder, ColumnDesign design) {
        if (design.getCompression() != null) {
            try {
                ColumnSchema.CompressionAlgorithm algorithm =
                        ColumnSchema.CompressionAlgorithm.valueOf(design.getCompression().toUpperCase());
                builder.compressionAlgorithm(algorithm);
            } catch (IllegalArgumentException e) {
                throw new RuntimeException("Unknown compression algorithm " + design.getCompression() + " for column " + name);
            }
        }
    }

    private void setEncoding(String name, ColumnSchema.ColumnSchemaBuilder builder, ColumnDesign design) {
        if (design.getEncoding() != null) {
            try {
                ColumnSchema.Encoding encoding =
                        ColumnSchema.Encoding.valueOf(design.getEncoding().toUpperCase());
                builder.encoding(encoding);
            } catch (IllegalArgumentException e) {
                throw new RuntimeException("Unknown encoding " + design.getEncoding() + " for column " + name);
            }
        }
    }

    private CreateTableOptions buildCreateTableOptions(Map<String, Object> properties) {
        CreateTableOptions options = new CreateTableOptions();

        Optional<PartitionDesign> optPartitionDesign = KuduTableProperties.getPartitionDesign(properties);
        if (optPartitionDesign.isPresent()) {
            PartitionDesign partitionDesign = optPartitionDesign.get();
            if (partitionDesign.getHash() != null) {
                for (HashPartition partition : partitionDesign.getHash()) {
                    options.addHashPartitions(partition.getColumns(), partition.getBuckets());
                }
            }
            if (partitionDesign.getRange() != null) {
                RangePartition range = partitionDesign.getRange();
                options.setRangePartitionColumns(range.getColumns());
            }
        }

        Optional<Integer> numReplicas = KuduTableProperties.getNumReplicas(properties);
        numReplicas.ifPresent(options::setNumReplicas);

        return options;
    }

    /**
     * translates TupleDomain to KuduPredicates.
     *
     * @return false if TupleDomain or one of its domains is none
     */
    private boolean addConstraintPredicates(KuduTable table, KuduScanToken.KuduScanTokenBuilder builder,
                                            TupleDomain<ColumnHandle> constraintSummary) {
        if (constraintSummary.isNone()) {
            return false;
        } else if (!constraintSummary.isAll()) {
            Schema schema = table.getSchema();
            for (TupleDomain.ColumnDomain<ColumnHandle> columnDomain : constraintSummary.getColumnDomains().get()) {
                int position = ((KuduColumnHandle) columnDomain.getColumn()).getOrdinalPosition();
                ColumnSchema columnSchema = schema.getColumnByIndex(position);
                Domain domain = columnDomain.getDomain();
                if (domain.isNone()) {
                    return false;
                } else if (domain.isAll()) {
                    // no restriction
                } else if (domain.isOnlyNull()) {
                    builder.addPredicate(KuduPredicate.newIsNullPredicate(columnSchema));
                } else if (domain.getValues().isAll() && domain.isNullAllowed()) {
                    builder.addPredicate(KuduPredicate.newIsNotNullPredicate(columnSchema));
                } else if (domain.isSingleValue()) {
                    KuduPredicate predicate = createEqualsPredicate(columnSchema, domain.getSingleValue());
                    builder.addPredicate(predicate);
                } else {
                    ValueSet valueSet = domain.getValues();
                    if (valueSet instanceof EquatableValueSet) {
                        DiscreteValues discreteValues = ((EquatableValueSet) valueSet).getDiscreteValues();
                        KuduPredicate predicate = createInListPredicate(columnSchema, discreteValues);
                        builder.addPredicate(predicate);
                    } else if (valueSet instanceof SortedRangeSet) {
                        Ranges ranges = ((SortedRangeSet) valueSet).getRanges();
                        Range span = ranges.getSpan();
                        Marker low = span.getLow();
                        if (!low.isLowerUnbounded()) {
                            KuduPredicate.ComparisonOp op = (low.getBound() == Marker.Bound.ABOVE)
                                    ? KuduPredicate.ComparisonOp.GREATER : KuduPredicate.ComparisonOp.GREATER_EQUAL;
                            KuduPredicate predicate = createComparisonPredicate(columnSchema, op, low.getValue());
                            builder.addPredicate(predicate);
                        }
                        Marker high = span.getHigh();
                        if (!high.isUpperUnbounded()) {
                            KuduPredicate.ComparisonOp op = (low.getBound() == Marker.Bound.BELOW)
                                    ? KuduPredicate.ComparisonOp.LESS : KuduPredicate.ComparisonOp.LESS_EQUAL;
                            KuduPredicate predicate = createComparisonPredicate(columnSchema, op, high.getValue());
                            builder.addPredicate(predicate);
                        }
                    } else {
                        throw new IllegalStateException("Unexpected domain: " + domain);
                    }
                }
            }
        }
        return true;
    }

    private KuduPredicate createInListPredicate(ColumnSchema columnSchema, DiscreteValues discreteValues) {
        KuduType kuduType = KuduType.fromKuduClientType(columnSchema.getType());
        List<Object> javaValues = discreteValues.getValues().stream().map(value -> kuduType.getJavaValue(value)).collect(toImmutableList());
        return KuduPredicate.newInListPredicate(columnSchema, javaValues);
    }

    private KuduPredicate createEqualsPredicate(ColumnSchema columnSchema, Object value) {
        return createComparisonPredicate(columnSchema, KuduPredicate.ComparisonOp.EQUAL, value);
    }

    private KuduPredicate createComparisonPredicate(ColumnSchema columnSchema,
                                                    KuduPredicate.ComparisonOp op,
                                                    Object value) {
        KuduType kuduType = KuduType.fromKuduClientType(columnSchema.getType());
        Object javaValue = kuduType.getJavaValue(value);
        if (javaValue instanceof Long) {
            return KuduPredicate.newComparisonPredicate(columnSchema, op, (Long) javaValue);
        } else if (javaValue instanceof Integer) {
            return KuduPredicate.newComparisonPredicate(columnSchema, op, (Integer) javaValue);
        } else if (javaValue instanceof Short) {
            return KuduPredicate.newComparisonPredicate(columnSchema, op, (Short) javaValue);
        } else if (javaValue instanceof Byte) {
            return KuduPredicate.newComparisonPredicate(columnSchema, op, (Byte) javaValue);
        } else if (javaValue instanceof String) {
            return KuduPredicate.newComparisonPredicate(columnSchema, op, (String) javaValue);
        } else if (javaValue instanceof Double) {
            return KuduPredicate.newComparisonPredicate(columnSchema, op, (Double) javaValue);
        } else if (javaValue instanceof Float) {
            return KuduPredicate.newComparisonPredicate(columnSchema, op, (Float) javaValue);
        } else if (javaValue instanceof Boolean) {
            return KuduPredicate.newComparisonPredicate(columnSchema, op, (Boolean) javaValue);
        } else if (javaValue instanceof byte[]) {
            return KuduPredicate.newComparisonPredicate(columnSchema, op, (byte[]) javaValue);
        } else if (javaValue == null) {
            throw new IllegalStateException("Unexpected null java value for column " + columnSchema.getName());
        } else {
            throw new IllegalStateException("Unexpected java value for column "
                    + columnSchema.getName() + ": " + javaValue + "(" + javaValue.getClass() + ")");
        }
    }

    private KuduSplit toKuduSplit(KuduTableHandle tableHandle, KuduScanToken token,
                                  int primaryKeyColumnCount) {
        try {
            byte[] pb = token.serialize();
            return new KuduSplit(tableHandle, primaryKeyColumnCount, pb);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private String toRawName(SchemaTableName schemaTableName) {
        String rawName;
        if (schemaTableName.getSchemaName().equals(NULL_SCHEMA)) {
            rawName = tenantPrefix + schemaTableName.getTableName();
        } else {
            rawName = tenantPrefix + schemaTableName.getSchemaName() + "." + schemaTableName.getTableName();
        }
        return rawName;
    }
}

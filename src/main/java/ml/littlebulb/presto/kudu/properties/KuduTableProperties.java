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
package ml.littlebulb.presto.kudu.properties;

import com.facebook.presto.spi.session.PropertyMetadata;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;
import org.apache.kudu.shaded.com.google.common.base.Predicates;
import org.apache.kudu.shaded.com.google.common.collect.Iterators;
import org.joda.time.format.ISODateTimeFormat;

import java.io.IOException;
import java.math.BigDecimal;
import java.text.NumberFormat;
import java.util.*;

import static com.facebook.presto.spi.session.PropertyMetadata.integerSessionProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.stringSessionProperty;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

/**
 * Class contains all table properties for the Kudu connector. Used when creating a table:
 * <pre>
 * CREATE TABLE foo (a VARCHAR, b INT)
 * WITH (
 * column_design = '{"a": {"key": true}, "b": {"encoding": "bitshuffle"}}',
 * partition_design = '{"hash":[{"columns":["a"], "buckets": 2}]}',
 * num_replicas = 1
 * );
 * </pre>
 * <br>or with range partitioning<br>
 * <pre>
 * CREATE TABLE foo (a VARCHAR, b INT)
 * WITH (
 * column_design = '{"a": {"key": true}}',
 * partition_design = '{"range":{"columns":["a"]}}',
 * range_partitions = '[{"lower": null, "upper": "Am"}, {"lower": "Am", "upper": "Bs"}]',
 * num_replicas = 1
 * );
 * </pre>
 */
public final class KuduTableProperties {
    public static final String COLUMN_DESIGN = "column_design";
    public static final String PARTITION_DESIGN = "partition_design";
    public static final String RANGE_PARTITIONS = "range_partitions";
    public static final String NUM_REPLICAS = "num_replicas";

    private static final ObjectMapper mapper = new ObjectMapper();

    private static final long DEFAULT_DEADLINE = 20000; // deadline for retrieving range partitions in milliseconds

    private final List<PropertyMetadata<?>> tableProperties;


    public KuduTableProperties() {
        PropertyMetadata<String> s1 = stringSessionProperty(
                COLUMN_DESIGN,
                "Kudu-specific column design (key, encoding, and compression) as JSON, like {\"column1\": {\"key\": true, \"encoding\": \"dictionary\", \"compression\": \"LZ4\"}, \"column2\": {...}}",
                null,
                false);

        PropertyMetadata<String> s2 = stringSessionProperty(
                PARTITION_DESIGN,
                "Partition design (hash partition(s) and/or range partition) as JSON.",
                null,
                false);

        PropertyMetadata<Integer> s3 = integerSessionProperty(
                NUM_REPLICAS,
                "Number of tablet replicas. Default 3.",
                3,
                false);

        PropertyMetadata<String> s4 = stringSessionProperty(
                RANGE_PARTITIONS,
                "Initial range partitions as JSON",
                null,
                false);

        tableProperties = ImmutableList.of(s1, s2, s3, s4);
    }

    public List<PropertyMetadata<?>> getTableProperties() {
        return tableProperties;
    }

    /**
     * Gets the value of the column_design property, or Optional.empty() if not set.
     *
     * @param tableProperties The map of table properties
     * @return The column design settings
     */
    public static Optional<Map<String, ColumnDesign>> getColumnDesign(
            Map<String, Object> tableProperties) {
        requireNonNull(tableProperties);

        @SuppressWarnings("unchecked")
        String json = (String) tableProperties.get(COLUMN_DESIGN);
        if (json == null) {
            return Optional.empty();
        }

        try {
            Map<String, ColumnDesign> map = mapper.readValue(json, new TypeReference<Map<String, ColumnDesign>>() {
            });
            return Optional.of(map);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Optional<PartitionDesign> getPartitionDesign(Map<String, Object> tableProperties) {
        requireNonNull(tableProperties);

        @SuppressWarnings("unchecked")
        String json = (String) tableProperties.get(PARTITION_DESIGN);
        try {
            PartitionDesign design = mapper.readValue(json, PartitionDesign.class);
            return Optional.of(design);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static List<RangePartition> getRangePartitions(Map<String, Object> tableProperties) {
        requireNonNull(tableProperties);

        @SuppressWarnings("unchecked")
        String json = (String) tableProperties.get(RANGE_PARTITIONS);
        if (json != null) {
            try {
                RangePartition[] partitions = mapper.readValue(json, RangePartition[].class);
                if (partitions == null) {
                    return ImmutableList.of();
                } else {
                    return ImmutableList.copyOf(partitions);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            return ImmutableList.of();
        }
    }

    public static Optional<Integer> getNumReplicas(Map<String, Object> tableProperties) {
        requireNonNull(tableProperties);

        @SuppressWarnings("unchecked")
        Integer numReplicas = (Integer) tableProperties.get(NUM_REPLICAS);
        return Optional.ofNullable(numReplicas);
    }

    public static Map<String, Object> toMap(KuduTable table) {
        Map<String, Object> properties = new HashMap<>();

        LinkedHashMap<String, ColumnDesign> columns = getColumns(table);

        PartitionDesign partitionDesign = getPartitionDesign(table);

        List<RangePartition> rangePartitionList = getRangePartitionList(table, DEFAULT_DEADLINE);

        try {
            String columnDesignValue = mapper.writeValueAsString(columns);
            properties.put(COLUMN_DESIGN, columnDesignValue);

            String partitionDesignValue = mapper.writeValueAsString(partitionDesign);
            properties.put(PARTITION_DESIGN, partitionDesignValue);

            String partitionRangesValue = mapper.writeValueAsString(rangePartitionList);
            properties.put(RANGE_PARTITIONS, partitionRangesValue);

            // currently no access to numReplicas?

            return properties;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static List<RangePartition> getRangePartitionList(KuduTable table, long deadline) {
        List<RangePartition> rangePartitions = new ArrayList();
        if (!table.getPartitionSchema().getRangeSchema().getColumns().isEmpty()) {
            try {
                Iterator var4 = table.getTabletsLocations(deadline).iterator();

                while (var4.hasNext()) {
                    LocatedTablet tablet = (LocatedTablet) var4.next();
                    Partition partition = tablet.getPartition();
                    if (Iterators.all(partition.getHashBuckets().iterator(), Predicates.equalTo(0))) {
                        RangePartition rangePartition = buildRangePartition(table, partition);
                        rangePartitions.add(rangePartition);
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        return rangePartitions;
    }

    private static RangePartition buildRangePartition(KuduTable table, Partition partition) throws Exception {
        RangePartition rangePartition = new RangePartition();

        RangeBoundValue lower = buildRangePartitionBound(table, partition.getRangeKeyStart());
        RangeBoundValue upper = buildRangePartitionBound(table, partition.getRangeKeyEnd());
        rangePartition.setLower(lower);
        rangePartition.setUpper(upper);

        return rangePartition;
    }

    private static RangeBoundValue buildRangePartitionBound(KuduTable table, byte[] rangeKey) throws Exception {
        if (rangeKey.length == 0) {
            return null;
        } else {
            Schema schema = table.getSchema();
            PartitionSchema partitionSchema = table.getPartitionSchema();
            PartitionSchema.RangeSchema rangeSchema = partitionSchema.getRangeSchema();
            List<Integer> rangeColumns = rangeSchema.getColumns();

            final int numColumns = rangeColumns.size();

            PartialRow bound = KeyEncoderAccessor.decodeRangePartitionKey(schema, partitionSchema, rangeKey);

            RangeBoundValue value = new RangeBoundValue();
            ArrayList<Object> list = new ArrayList<>();
            for (int i = 0; i < numColumns; i++) {
                Object obj = toValue(schema, bound, rangeColumns.get(i));
                list.add(obj);
            }
            value.setValues(list);
            return value;
        }
    }

    private static Object toValue(Schema schema, PartialRow bound, Integer idx) {
        Type type = schema.getColumnByIndex(idx).getType();
        switch (type) {
            case UNIXTIME_MICROS:
                long millis = bound.getLong(idx) / 1000;
                return ISODateTimeFormat.dateTimeParser().print(millis);
            case STRING:
                return bound.getString(idx);
            case INT64:
                return bound.getLong(idx);
            case INT32:
                return bound.getInt(idx);
            case INT16:
                return bound.getShort(idx);
            case INT8:
                short s = bound.getByte(idx);
                return s;
            case BOOL:
                return bound.getBoolean(idx);
            case BINARY:
                return javax.xml.bind.DatatypeConverter.printHexBinary(bound.getBinaryCopy(idx));
            default:
                throw new IllegalStateException("Unhandled type " + type + " for range partition");
        }
    }

    private static LinkedHashMap<String, ColumnDesign> getColumns(KuduTable table) {
        Schema schema = table.getSchema();
        LinkedHashMap<String, ColumnDesign> columns = new LinkedHashMap<>();
        for (ColumnSchema columnSchema : schema.getColumns()) {
            ColumnDesign design = new ColumnDesign();
            design.setNullable(columnSchema.isNullable());
            design.setKey(columnSchema.isKey());
            if (columnSchema.getCompressionAlgorithm() != null) {
                design.setCompression(columnSchema.getCompressionAlgorithm().name());
            }
            if (columnSchema.getEncoding() != null) {
                design.setEncoding(columnSchema.getEncoding().name());
            }
            columns.put(columnSchema.getName(), design);
        }
        return columns;
    }

    private static PartitionDesign getPartitionDesign(KuduTable table) {
        Schema schema = table.getSchema();
        PartitionDesign partitionDesign = new PartitionDesign();
        PartitionSchema partitionSchema = table.getPartitionSchema();

        List<HashPartitionDefinition> hashPartitions = partitionSchema.getHashBucketSchemas().stream()
                .map(hashBucketSchema -> {
                    HashPartitionDefinition hash = new HashPartitionDefinition();
                    List<String> cols = hashBucketSchema.getColumnIds().stream()
                            .map(idx -> schema.getColumnByIndex(idx).getName()).collect(toImmutableList());
                    hash.setColumns(cols);
                    hash.setBuckets(hashBucketSchema.getNumBuckets());
                    return hash;
                }).collect(toImmutableList());
        partitionDesign.setHash(hashPartitions);

        List<Integer> rangeColumns = partitionSchema.getRangeSchema().getColumns();
        if (!rangeColumns.isEmpty()) {
            RangePartitionDefinition definition = new RangePartitionDefinition();
            definition.setColumns(rangeColumns.stream()
                    .map(i -> schema.getColumns().get(i).getName())
                    .collect(ImmutableList.toImmutableList()));
            partitionDesign.setRange(definition);
        }

        return partitionDesign;
    }

    public static PartialRow toRangeBoundToPartialRow(Schema schema, RangePartitionDefinition definition,
                                                      RangeBoundValue boundValue) {
        PartialRow partialRow = new PartialRow(schema);
        if (boundValue != null) {
            List<Integer> rangeColumns = definition.getColumns().stream()
                    .map(name -> schema.getColumnIndex(name)).collect(toImmutableList());

            if (rangeColumns.size() != boundValue.getValues().size()) {
                throw new IllegalStateException("Expected " + rangeColumns.size()
                        + " range columns, but got " + boundValue.getValues().size());
            }
            for (int i = 0; i < rangeColumns.size(); i++) {
                Object obj = boundValue.getValues().get(i);
                int idx = rangeColumns.get(i);
                ColumnSchema columnSchema = schema.getColumnByIndex(idx);
                setColumnValue(partialRow, idx, obj, columnSchema.getType(), columnSchema.getName());
            }
        }
        return partialRow;
    }

    private static void setColumnValue(PartialRow partialRow, int idx, Object obj, Type type, String name) {
        Number n;

        switch (type) {
            case STRING:
                if (obj instanceof String) {
                    partialRow.addString(idx, (String) obj);
                } else {
                    handleInvalidValue(name, type, obj);
                }
                break;
            case INT64:
                n = toNumber(obj, type, name);
                partialRow.addLong(idx, n.longValue());
                break;
            case INT32:
                n = toNumber(obj, type, name);
                partialRow.addInt(idx, n.intValue());
                break;
            case INT16:
                n = toNumber(obj, type, name);
                partialRow.addShort(idx, n.shortValue());
                break;
            case INT8:
                n = toNumber(obj, type, name);
                partialRow.addByte(idx, n.byteValue());
                break;
            case DOUBLE:
                n = toNumber(obj, type, name);
                partialRow.addDouble(idx, n.doubleValue());
                break;
            case FLOAT:
                n = toNumber(obj, type, name);
                partialRow.addFloat(idx, n.floatValue());
                break;
            case UNIXTIME_MICROS:
                long l = toUnixTimeMicros(obj, type, name);
                partialRow.addLong(idx, l);
                break;
            case BOOL:
                boolean b = toBoolean(obj, type, name);
                partialRow.addBoolean(idx, b);
                break;
            case BINARY:
                byte[] bytes = toByteArray(obj, type, name);
                partialRow.addBinary(idx, bytes);
                break;
            default:
                handleInvalidValue(name, type, obj);
                break;
        }
    }

    private static byte[] toByteArray(Object obj, Type type, String name) {
        if (obj instanceof byte[]) {
            return (byte[]) obj;
        } else if (obj instanceof String) {
            Base64.Decoder decoder = Base64.getDecoder();
            return decoder.decode((String) obj);
        } else {
            handleInvalidValue(name, type, obj);
            return null;
        }
    }

    private static boolean toBoolean(Object obj, Type type, String name) {
        if (obj instanceof Boolean) {
            return (Boolean) obj;
        } else if (obj instanceof String) {
            return Boolean.valueOf((String) obj);
        } else {
            handleInvalidValue(name, type, obj);
            return false;
        }
    }

    private static long toUnixTimeMicros(Object obj, Type type, String name) {
        if (Number.class.isAssignableFrom(obj.getClass())) {
            return ((Number) obj).longValue();
        } else if (obj instanceof String) {
            String s = (String) obj;
            s = s.trim().replace(' ', 'T');
            long millis = ISODateTimeFormat.dateOptionalTimeParser().parseMillis(s);
            return millis * 1000;
        } else {
            handleInvalidValue(name, type, obj);
            return 0;
        }
    }

    private static Number toNumber(Object obj, Type type, String name) {
        if (Number.class.isAssignableFrom(obj.getClass())) {
            return (Number) obj;
        } else if (obj instanceof String) {
            String s = (String) obj;
            BigDecimal d = new BigDecimal((String) obj);
            return d;
        } else {
            handleInvalidValue(name, type, obj);
            return 0;
        }
    }

    private static void handleInvalidValue(String name, Type type, Object obj) {
        throw new IllegalStateException("Invalid value " + obj + " for column " + name + " of type " + type);
    }
}

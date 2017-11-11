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
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.PartitionSchema;

import java.io.IOException;
import java.util.*;

import static com.facebook.presto.spi.session.PropertyMetadata.integerSessionProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.stringSessionProperty;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

/**
 * Class contains all table properties for the Kudu connector. Used when creating a table:
 * <p>
 * CREATE TABLE foo (a VARCHAR, b INT)
 * WITH (
 * column_design = '{"a": {"key": true}, "b": {"encoding": "bitshuffle"}}',
 * partition_design = '{"hash":[{"columns":["a"], "buckets": 2}]}',
 * num_replicas = 1
 * );
 */
public final class KuduTableProperties {
    public static final String COLUMN_DESIGN = "column_design";
    public static final String PARTITION_DESIGN = "partition_design";
    public static final String NUM_REPLICAS = "num_replicas";

    private static final ObjectMapper mapper = new ObjectMapper();

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

        tableProperties = ImmutableList.of(s1, s2, s3);
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

    public static Optional<Integer> getNumReplicas(Map<String, Object> tableProperties) {
        requireNonNull(tableProperties);

        @SuppressWarnings("unchecked")
        Integer numReplicas = (Integer) tableProperties.get(NUM_REPLICAS);
        return Optional.ofNullable(numReplicas);
    }

    public static Map<String, Object> toMap(KuduTable table) {
        Map<String, Object> properties = new HashMap<>();
        final Schema schema = table.getSchema();
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

        PartitionDesign partitionDesign = new PartitionDesign();
        PartitionSchema partitionSchema = table.getPartitionSchema();
        List<HashPartition> hashPartitions = partitionSchema.getHashBucketSchemas().stream()
                .map(hashBucketSchema -> {
                    HashPartition hash = new HashPartition();
                    List<String> cols = hashBucketSchema.getColumnIds().stream()
                            .map(idx -> schema.getColumnByIndex(idx).getName()).collect(toImmutableList());
                    hash.setColumns(cols);
                    hash.setBuckets(hashBucketSchema.getNumBuckets());
                    return hash;
                }).collect(toImmutableList());
        partitionDesign.setHash(hashPartitions);

        try {
            String columnDesignValue = mapper.writeValueAsString(columns);
            properties.put(COLUMN_DESIGN, columnDesignValue);

            String partitionDesignValue = mapper.writeValueAsString(partitionDesign);
            properties.put(PARTITION_DESIGN, partitionDesignValue);

            // currently no access to numReplicas?

            return properties;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}

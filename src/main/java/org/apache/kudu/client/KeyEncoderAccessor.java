package org.apache.kudu.client;

import org.apache.kudu.Schema;

/**
 * Little wrapper to access KeyEncoder in Kudu Java client.
 */
public class KeyEncoderAccessor {
    private KeyEncoderAccessor() {
    }

    public static byte[] encodePrimaryKey(PartialRow row){
        return KeyEncoder.encodePrimaryKey(row);
    }

    public static PartialRow decodePrimaryKey(Schema schema, byte[] key) {
        return KeyEncoder.decodePrimaryKey(schema, key);
    }

    public static byte[] encodeRangePartitionKey(PartialRow row, PartitionSchema.RangeSchema rangeSchema) {
        return KeyEncoder.encodeRangePartitionKey(row, rangeSchema);
    }

    public static PartialRow decodeRangePartitionKey(Schema schema, PartitionSchema partitionSchema, byte[] key) {
        return KeyEncoder.decodeRangePartitionKey(schema, partitionSchema, key);
    }
}

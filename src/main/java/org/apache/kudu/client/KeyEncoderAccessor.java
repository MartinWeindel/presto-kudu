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
}

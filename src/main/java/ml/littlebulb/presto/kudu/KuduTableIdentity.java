package ml.littlebulb.presto.kudu;

import com.facebook.presto.spi.TableIdentity;
import com.google.common.base.Charsets;

public class KuduTableIdentity implements TableIdentity {
    private final String tableId;

    public KuduTableIdentity(String tableId) {
        this.tableId = tableId;
    }

    public String getTableId() {
        return tableId;
    }

    @Override
    public byte[] serialize() {
        return tableId.getBytes(Charsets.UTF_8);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof KuduTableIdentity) {
            return tableId.equals(((KuduTableIdentity) obj).tableId);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return tableId.hashCode();
    }

    public static KuduTableIdentity deserialize(byte[] bytes) {
        return new KuduTableIdentity(new String(bytes, Charsets.UTF_8));
    }
}

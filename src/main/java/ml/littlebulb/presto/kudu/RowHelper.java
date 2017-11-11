package ml.littlebulb.presto.kudu;

import com.google.common.base.Charsets;
import org.apache.kudu.Schema;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.RowResult;

public class RowHelper {
    private RowHelper() {
    }

    public static void copyPrimaryKey(Schema schema, RowResult from, PartialRow to) {
        for (int i = 0; i < schema.getPrimaryKeyColumnCount(); i++) {
            switch (schema.getColumnByIndex(i).getType()) {
                case STRING:
                    to.addStringUtf8(i, from.getString(i).getBytes(Charsets.UTF_8));
                    break;
                case INT64:
                case UNIXTIME_MICROS:
                    to.addLong(i, from.getLong(i));
                    break;
                case INT32:
                    to.addInt(i, from.getInt(i));
                    break;
                case INT16:
                    to.addShort(i, from.getShort(i));
                    break;
                case INT8:
                    to.addByte(i, from.getByte(i));
                    break;
                case DOUBLE:
                    to.addDouble(i, from.getDouble(i));
                    break;
                case FLOAT:
                    to.addFloat(i, from.getFloat(i));
                    break;
                case BOOL:
                    to.addBoolean(i, from.getBoolean(i));
                    break;
                case BINARY:
                    to.addBinary(i, from.getBinary(i));
                    break;
                default:
                    throw new IllegalStateException("Unknown type " + schema.getColumnByIndex(i).getType()
                            + " for column " + schema.getColumnByIndex(i).getName());
            }
        }
    }

    public static void copyPrimaryKey(Schema schema, PartialRow from, PartialRow to) {
        for (int i = 0; i < schema.getPrimaryKeyColumnCount(); i++) {
            switch (schema.getColumnByIndex(i).getType()) {
                case STRING:
                    to.addStringUtf8(i, from.getString(i).getBytes(Charsets.UTF_8));
                    break;
                case INT64:
                case UNIXTIME_MICROS:
                    to.addLong(i, from.getLong(i));
                    break;
                case INT32:
                    to.addInt(i, from.getInt(i));
                    break;
                case INT16:
                    to.addShort(i, from.getShort(i));
                    break;
                case INT8:
                    to.addByte(i, from.getByte(i));
                    break;
                case DOUBLE:
                    to.addDouble(i, from.getDouble(i));
                    break;
                case FLOAT:
                    to.addFloat(i, from.getFloat(i));
                    break;
                case BOOL:
                    to.addBoolean(i, from.getBoolean(i));
                    break;
                case BINARY:
                    to.addBinary(i, from.getBinary(i));
                    break;
                default:
                    throw new IllegalStateException("Unknown type " + schema.getColumnByIndex(i).getType()
                            + " for column " + schema.getColumnByIndex(i).getName());
            }
        }
    }
}

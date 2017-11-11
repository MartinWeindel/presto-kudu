package ml.littlebulb.presto.kudu;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.kudu.Schema;
import org.apache.kudu.client.KeyEncoderAccessor;
import org.apache.kudu.client.KuduScanner;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.PartialRow;

import java.util.List;
import java.util.Map;

public class KuduRecordCursorWithVirtualRowId extends KuduRecordCursor {
    private final KuduTable table;
    private final Map<Integer, Integer> fieldMapping;

    public KuduRecordCursorWithVirtualRowId(KuduScanner scanner, KuduTable table,
                                            List<KuduType> kuduColumnTypes,
                                            Map<Integer, Integer> fieldMapping) {
        super(scanner, kuduColumnTypes);
        this.table = table;
        this.fieldMapping = fieldMapping;
    }

    @Override
    protected int mapping(int field) {
        return fieldMapping.get(field);
    }

    @Override
    public Slice getSlice(int field) {
        if (fieldMapping.get(field) == -1) {
            PartialRow partialRow = buildPrimaryKey();
            return Slices.wrappedBuffer(KeyEncoderAccessor.encodePrimaryKey(partialRow));
        } else {
            return super.getSlice(field);
        }
    }

    private PartialRow buildPrimaryKey() {
        Schema schema = table.getSchema();
        PartialRow row = new PartialRow(schema);
        RowHelper.copyPrimaryKey(schema, currentRow, row);
        return row;
    }
}

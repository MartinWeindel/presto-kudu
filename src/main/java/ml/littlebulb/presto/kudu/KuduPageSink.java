package ml.littlebulb.presto.kudu;

import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.SqlDate;
import com.facebook.presto.spi.type.SqlDecimal;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.SessionConfiguration;
import org.apache.kudu.client.Upsert;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.Varchars.isVarcharType;
import static java.lang.Float.intBitsToFloat;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class KuduPageSink implements ConnectorPageSink {
    private final ConnectorSession connectorSession;
    private final KuduSession session;
    private final KuduTable table;
    private final List<Type> columnTypes;
    private final List<Type> originalColumnTypes;
    private final boolean generateUUID;

    private final String uuid;
    private int nextSubId = 0;

    public KuduPageSink(ConnectorSession connectorSession, KuduClientSession clientSession,
                        KuduExtendedTableHandle extendedTableHandle,
                        boolean generateUUID) {
        requireNonNull(clientSession, "clientSession is null");
        this.connectorSession = connectorSession;
        this.columnTypes = extendedTableHandle.getColumnTypes();
        this.originalColumnTypes = extendedTableHandle.getOriginalColumnTypes();
        this.generateUUID = generateUUID;

        this.table = extendedTableHandle.getTable(clientSession);
        this.session = clientSession.newSession();
        this.session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND);
        uuid = UUID.randomUUID().toString();
    }

    @Override
    public CompletableFuture<?> appendPage(Page page) {
        for (int position = 0; position < page.getPositionCount(); position++) {
            Upsert upsert = table.newUpsert();
            PartialRow row = upsert.getRow();
            int start = 0;
            if (generateUUID) {
                String id = String.format("%s-%08x", uuid, nextSubId++);
                row.addString(0, id);
                start = 1;
            }

            for (int channel = 0; channel < page.getChannelCount(); channel++) {
                appendColumn(row, page, position, channel, channel + start);
            }

            try {
                session.apply(upsert);
            } catch (KuduException e) {
                throw new RuntimeException(e);
            }
        }
        return NOT_BLOCKED;
    }

    private void appendColumn(PartialRow row, Page page, int position, int channel, int destChannel) {
        Block block = page.getBlock(channel);
        Type type = columnTypes.get(destChannel);
        if (block.isNull(position)) {
            row.setNull(destChannel);
        } else if (TIMESTAMP.equals(type)) {
            row.addLong(destChannel, type.getLong(block, position) * 1000);
        } else if (REAL.equals(type)) {
            row.addFloat(destChannel, intBitsToFloat((int) type.getLong(block, position)));
        } else if (BIGINT.equals(type)) {
            row.addLong(destChannel, type.getLong(block, position));
        } else if (INTEGER.equals(type)) {
            row.addInt(destChannel, (int) type.getLong(block, position));
        } else if (SMALLINT.equals(type)) {
            row.addShort(destChannel, (short) type.getLong(block, position));
        } else if (TINYINT.equals(type)) {
            row.addByte(destChannel, (byte) type.getLong(block, position));
        } else if (BOOLEAN.equals(type)) {
            row.addBoolean(destChannel, type.getBoolean(block, position));
        } else if (DOUBLE.equals(type)) {
            row.addDouble(destChannel, type.getDouble(block, position));
        } else if (isVarcharType(type)) {
            Type originalType = originalColumnTypes.get(destChannel);
            if (DATE.equals(originalType)) {
                SqlDate date = (SqlDate) originalType.getObjectValue(connectorSession, block, position);
                LocalDateTime ldt = LocalDateTime.ofEpochSecond(TimeUnit.DAYS.toSeconds(date.getDays()), 0, ZoneOffset.UTC);
                byte[] bytes = ldt.format(DateTimeFormatter.ISO_LOCAL_DATE).getBytes(Charsets.UTF_8);
                row.addStringUtf8(destChannel, bytes);
            } else {
                row.addString(destChannel, type.getSlice(block, position).toStringUtf8());
            }
        } else if (VARBINARY.equals(type)) {
            row.addBinary(destChannel, type.getSlice(block, position).toByteBuffer());
        } else if (type instanceof DecimalType) {
            SqlDecimal sqlDecimal = (SqlDecimal) type.getObjectValue(connectorSession, block, position);
            row.addDecimal(destChannel, sqlDecimal.toBigDecimal());
        } else {
            throw new UnsupportedOperationException("Type is not supported: " + type);
        }
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish() {
        closeSession();
        // the committer does not need any additional info
        return completedFuture(ImmutableList.of());
    }

    @Override
    public void abort() {
        closeSession();
    }

    private void closeSession() {
        try {
            session.close();
        } catch (KuduException e) {
            throw new RuntimeException(e);
        }
    }
}

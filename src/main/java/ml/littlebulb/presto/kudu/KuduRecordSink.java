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
package ml.littlebulb.presto.kudu;

import com.facebook.presto.spi.RecordSink;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import org.apache.kudu.client.*;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.Varchars.isVarcharType;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.Float.intBitsToFloat;
import static java.util.Objects.requireNonNull;

public class KuduRecordSink implements RecordSink {
    private final KuduSession session;
    private final KuduTable table;
    private final List<Type> columnTypes;
    private final List<Type> originalColumnTypes;
    private final boolean generateUUID;

    private Upsert upsert = null;
    private int field = -1;
    private final String uuid;
    private int nextSubId = 0;

    public KuduRecordSink(KuduClientSession clientSession,
                          KuduExtendedTableHandle extendedTableHandle,
                          boolean generateUUID) {
        requireNonNull(clientSession, "clientSession is null");
        this.columnTypes = extendedTableHandle.getColumnTypes();
        this.originalColumnTypes = extendedTableHandle.getOriginalColumnTypes();
        this.generateUUID = generateUUID;

        this.table = extendedTableHandle.getTable(clientSession);
        session = clientSession.newSession();
        session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND);
        uuid = UUID.randomUUID().toString();
    }

    @Override
    public void beginRecord() {
        checkState(upsert == null, "already in record");

        upsert = table.newUpsert();
        field = -1;
        if (generateUUID) {
            String id = String.format("%s-%08x", uuid, nextSubId++);
            appendString(id.getBytes());
        }
    }

    @Override
    public void finishRecord() {
        checkState(upsert != null, "not in record");
        checkState(field == columnTypes.size() - 1, "not all fields set");
        try {
            session.apply(upsert);
        } catch (KuduException e) {
            throw new RuntimeException(e);
        }
        field = -1;
        upsert = null;
    }

    private PartialRow rowNextColumn() {
        field++;
        return upsert.getRow();
    }

    @Override
    public void appendNull() {
        rowNextColumn().setNull(field);
    }

    @Override
    public void appendBoolean(boolean value) {
        rowNextColumn().addBoolean(field, value);
    }

    @Override
    public void appendLong(long value) {
        PartialRow row = rowNextColumn();
        Type columnType = columnTypes.get(field);
        Type originalColumnType = originalColumnTypes.get(field);
        if (TIMESTAMP.equals(columnType)) {
            row.addLong(field, value * 1000);
        } else if (INTEGER.equals(columnType)) {
            row.addInt(field, (int) value);
        } else if (REAL.equals(columnType)) {
            row.addFloat(field, intBitsToFloat((int) value));
        } else if (BIGINT.equals(columnType)) {
            row.addLong(field, value);
        } else if (DATE.equals(originalColumnType)) {
            LocalDateTime ldt = LocalDateTime.ofEpochSecond(TimeUnit.DAYS.toSeconds(value), 0, ZoneOffset.UTC);
            byte[] bytes = ldt.format(DateTimeFormatter.ISO_LOCAL_DATE).getBytes(Charsets.UTF_8);
            row.addStringUtf8(field, bytes);
        } else {
            throw new UnsupportedOperationException("Type is not supported: " + columnType);
        }
    }

    @Override
    public void appendDouble(double value) {
        rowNextColumn().addDouble(field, value);
    }

    @Override
    public void appendBigDecimal(BigDecimal value) {
        throw new IllegalStateException("BigDecimal not supported in Kudu");
    }

    @Override
    public void appendString(byte[] value) {
        PartialRow row = rowNextColumn();
        Type columnType = columnTypes.get(field);
        if (VARBINARY.equals(columnType)) {
            row.addBinary(field, value);
        } else if (isVarcharType(columnType)) {
            row.addStringUtf8(field, value);
        } else {
            throw new UnsupportedOperationException("Type is not supported: " + columnType);
        }
    }

    @Override
    public void appendObject(Object value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Collection<Slice> commit() {
        checkState(upsert == null, "record not finished");
        closeSession();
        // the committer does not need any additional info
        return ImmutableList.of();
    }

    @Override
    public void rollback() {
        closeSession();
    }

    private void closeSession() {
        try {
            session.close();
        } catch (KuduException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<Type> getColumnTypes() {
        if (generateUUID) {
            return originalColumnTypes.subList(1, columnTypes.size());
        } else {
            return originalColumnTypes;
        }
    }
}

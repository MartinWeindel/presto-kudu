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

import com.facebook.presto.spi.predicate.NullableValue;
import com.facebook.presto.spi.type.*;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.RowResult;

import java.nio.ByteBuffer;

import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Float.intBitsToFloat;
import static java.util.Objects.requireNonNull;

public enum KuduType {
    STRING(VarcharType.VARCHAR, String.class, org.apache.kudu.Type.STRING),
    UNIXTIME_MICROS(TimestampType.TIMESTAMP, Long.class, org.apache.kudu.Type.UNIXTIME_MICROS),
    INT64(BigintType.BIGINT, Long.class, org.apache.kudu.Type.INT64),
    INT32(IntegerType.INTEGER, Integer.class, org.apache.kudu.Type.INT32),
    INT16(SmallintType.SMALLINT, Short.class, org.apache.kudu.Type.INT16),
    INT8(TinyintType.TINYINT, Byte.class, org.apache.kudu.Type.INT8),
    FLOAT(RealType.REAL, Float.class, org.apache.kudu.Type.FLOAT),
    DOUBLE(DoubleType.DOUBLE, Double.class, org.apache.kudu.Type.DOUBLE),
    BOOL(BooleanType.BOOLEAN, Boolean.class, org.apache.kudu.Type.BOOL),
    BINARY(VarbinaryType.VARBINARY, ByteBuffer.class, org.apache.kudu.Type.BINARY),
    DATE(DateType.DATE, String.class, org.apache.kudu.Type.STRING);

    private final Type prestoType;
    private final Class<?> javaType;
    private final org.apache.kudu.Type kuduClientType;

    KuduType(Type prestoType, Class<?> javaType, org.apache.kudu.Type kuduClientType) {
        this.prestoType = requireNonNull(prestoType, "prestoType is null");
        this.javaType = javaType;
        this.kuduClientType = kuduClientType;
    }

    public Type getPrestoType() {
        return prestoType;
    }

    public org.apache.kudu.Type getKuduClientType() { return kuduClientType; }

    public static KuduType fromKuduClientType(org.apache.kudu.Type ktype) {
        switch (ktype) {
            case STRING:
                return STRING;
            case UNIXTIME_MICROS:
                return UNIXTIME_MICROS;
            case INT64:
                return INT64;
            case INT32:
                return INT32;
            case INT16:
                return INT16;
            case INT8:
                return INT8;
            case FLOAT:
                return FLOAT;
            case DOUBLE:
                return DOUBLE;
            case BOOL:
                return BOOL;
            case BINARY:
                return BINARY;
            default:
                throw new IllegalStateException("Kudu type not implemented for " + ktype);
        }
    }

    public static NullableValue getColumnValue(PartialRow row, int i, KuduType kuduType) {
        Type prestoType = kuduType.getPrestoType();
        if (row.isNull(i)) {
            return NullableValue.asNull(prestoType);
        } else {
            switch (kuduType) {
                case STRING:
                    return NullableValue.of(prestoType, utf8Slice(row.getString(i)));
                case UNIXTIME_MICROS:
                    return NullableValue.of(prestoType, row.getLong(i) / 1000);
                case INT64:
                    return NullableValue.of(prestoType, row.getLong(i));
                case INT32:
                    return NullableValue.of(prestoType, row.getInt(i));
                case INT16:
                    return NullableValue.of(prestoType, row.getShort(i));
                case INT8:
                    return NullableValue.of(prestoType, row.getByte(i));
                case DOUBLE:
                    return NullableValue.of(prestoType, row.getDouble(i));
                case FLOAT:
                    return NullableValue.of(prestoType, (long) floatToRawIntBits(row.getFloat(i)));
                case BOOL:
                    return NullableValue.of(prestoType, row.getBoolean(i));
                case BINARY:
                    return NullableValue.of(prestoType, wrappedBuffer(row.getBinary(i)));
                default:
                    throw new IllegalStateException("Handling of type " + kuduType
                            + " is not implemented");
            }
        }
    }

    public Object getJavaValue(Object nativeValue) {
        switch (this) {
            case STRING:
                return ((Slice) nativeValue).toStringUtf8();
            case INT64:
            case BOOL:
            case DOUBLE:
                return nativeValue;
            case INT32:
                return ((Long) nativeValue).intValue();
            case INT16:
                return ((Long) nativeValue).shortValue();
            case INT8:
                return ((Long) nativeValue).byteValue();
            case FLOAT:
                // conversion can result in precision lost
                return intBitsToFloat(((Long) nativeValue).intValue());
            case UNIXTIME_MICROS:
                return ((Long) nativeValue) * 1000;
            case BINARY:
                return ((Slice) nativeValue).toByteBuffer();
            default:
                throw new IllegalStateException("Back conversion not implemented for " + this);
        }
    }


    public Object getFieldValue(RowResult row, int field) {
        if (row.isNull(field)) {
            return null;
        } else {
            switch (this) {
                case STRING:
                    return row.getString(field);
                case INT64:
                    return row.getLong(field);
                case BOOL:
                    return row.getBoolean(field);
                case DOUBLE:
                    return row.getDouble(field);
                case INT32:
                    return row.getInt(field);
                case INT16:
                    return row.getShort(field);
                case INT8:
                    return row.getByte(field);
                case FLOAT:
                    return row.getFloat(field);
                case UNIXTIME_MICROS:
                    return row.getLong(field) / 1000;
                case BINARY:
                    return Slices.wrappedBuffer(row.getBinary(field));
                default:
                    throw new IllegalStateException("field accessor not implemented for " + this);
            }
        }
    }

    public static KuduType fromPrestoType(Type type) {
        for (KuduType ktype : KuduType.values()) {
            if (ktype.getPrestoType().equals(type)) {
                return ktype;
            }
        }

        if (type instanceof VarcharType) {
            return KuduType.STRING;
        }

        throw new IllegalArgumentException("unsupported type: " + type);
    }
}
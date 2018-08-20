package ml.littlebulb.presto.kudu.procedures;

import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.procedure.Procedure;
import com.facebook.presto.spi.procedure.Procedure.Argument;
import com.google.common.collect.ImmutableList;
import ml.littlebulb.presto.kudu.KuduClientSession;
import ml.littlebulb.presto.kudu.properties.KuduTableProperties;
import ml.littlebulb.presto.kudu.properties.RangePartition;

import java.lang.invoke.MethodHandle;

import javax.inject.Inject;

import static com.facebook.presto.spi.block.MethodHandleUtil.methodHandle;
import static com.facebook.presto.spi.type.StandardTypes.VARCHAR;
import static java.util.Objects.requireNonNull;

public class RangePartitionProcedures {
    private static final MethodHandle ADD = methodHandle(RangePartitionProcedures.class, "addRangePartition",
            String.class, String.class, String.class);
    private static final MethodHandle DROP = methodHandle(RangePartitionProcedures.class, "dropRangePartition",
            String.class, String.class, String.class);

    private final KuduClientSession clientSession;

    @Inject
    public RangePartitionProcedures(KuduClientSession clientSession) {
        this.clientSession = requireNonNull(clientSession);
    }

    public Procedure getAddPartitionProcedure() {
        return new Procedure(
                "system",
                "add_range_partition",
                ImmutableList.of(new Argument("schema", VARCHAR), new Argument("table", VARCHAR),
                        new Argument("range_bounds", VARCHAR)),
                ADD.bindTo(this));
    }

    public Procedure getDropPartitionProcedure() {
        return new Procedure(
                "system",
                "drop_range_partition",
                ImmutableList.of(new Argument("schema", VARCHAR), new Argument("table", VARCHAR),
                        new Argument("range_bounds", VARCHAR)),
                DROP.bindTo(this));
    }

    public void addRangePartition(String schema, String table, String rangeBounds) {
        SchemaTableName schemaTableName = new SchemaTableName(schema, table);
        RangePartition rangePartition = KuduTableProperties.parseRangePartition(rangeBounds);
        clientSession.addRangePartition(schemaTableName, rangePartition);
    }

    public void dropRangePartition(String schema, String table, String rangeBounds) {
        SchemaTableName schemaTableName = new SchemaTableName(schema, table);
        RangePartition rangePartition = KuduTableProperties.parseRangePartition(rangeBounds);
        clientSession.dropRangePartition(schemaTableName, rangePartition);
    }
}

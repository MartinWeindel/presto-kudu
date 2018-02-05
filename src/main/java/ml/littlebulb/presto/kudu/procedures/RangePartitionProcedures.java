package ml.littlebulb.presto.kudu.procedures;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.procedure.Procedure;
import com.facebook.presto.spi.procedure.Procedure.Argument;
import com.google.common.collect.ImmutableList;
import ml.littlebulb.presto.kudu.KuduClientSession;
import org.apache.kudu.client.PartialRow;

import javax.inject.Inject;

import java.lang.invoke.MethodHandle;

import static com.facebook.presto.spi.block.MethodHandleUtil.methodHandle;
import static com.facebook.presto.spi.type.StandardTypes.ROW;
import static com.facebook.presto.spi.type.StandardTypes.VARCHAR;
import static java.util.Objects.requireNonNull;

public class RangePartitionProcedures {
    private static final MethodHandle CREATE = methodHandle(RangePartitionProcedures.class, "createRangePartition",
            String.class, String.class, String.class, String.class);
    private static final MethodHandle DELETE = methodHandle(RangePartitionProcedures.class, "deleteRangePartition",
            String.class, String.class, String.class, String.class);

    private final KuduClientSession clientSession;

    @Inject
    public RangePartitionProcedures(KuduClientSession clientSession) {
        this.clientSession = requireNonNull(clientSession);
    }

    public Procedure getCreatePartitionProcedure() {
        return new Procedure(
                "rangepartitions",
                "create",
                ImmutableList.of(new Argument("schema", VARCHAR), new Argument("table", VARCHAR),
                        new Argument("lower_bound", ROW), new Argument("upper_bound", ROW)),
                CREATE.bindTo(this));
    }

    public Procedure getDeletePartitionProcedure() {
        return new Procedure(
                "rangepartitions",
                "delete",
                ImmutableList.of(new Argument("schema", VARCHAR), new Argument("table", VARCHAR),
                        new Argument("lower_bound", ROW), new Argument("upper_bound", ROW)),
                DELETE.bindTo(this));
    }

    public void createRangePartition(String schema, String table, String lowerBound, String upperBound) {
        PartialRow kuduLower = toPartialRow(lowerBound);
        PartialRow kuduUpper = toPartialRow(upperBound);
        //clientSession.createRangePartition(schema, table, kuduLower, kuduUpper);
    }

    private PartialRow toPartialRow(String block) {
        return null; // TODO
    }

    public void deleteRangePartition(String schema, String table, String lowerBound, String upperBound) {
        PartialRow kuduLower = toPartialRow(lowerBound);
        PartialRow kuduUpper = toPartialRow(upperBound);
        //clientSession.deleteRangePartition(schema, table, kuduLower, kuduUpper);
    }
}

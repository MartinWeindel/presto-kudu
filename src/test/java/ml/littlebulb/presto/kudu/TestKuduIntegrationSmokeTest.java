package ml.littlebulb.presto.kudu;

import com.facebook.presto.spi.type.VarcharType;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.tests.AbstractTestIntegrationSmokeTest;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static ml.littlebulb.presto.kudu.KuduQueryRunner.createKuduQueryRunner;
import static io.airlift.tpch.TpchTable.ORDERS;

/**
 * Kudu master server is expected to be running on localhost. At least one
 * Kudu tablet server must be running, too.
 * With Docker, use e.g.
 *   "docker run --rm -d --name apache-kudu --net=host usuresearch/apache-kudu"
 */
public class TestKuduIntegrationSmokeTest extends AbstractTestIntegrationSmokeTest {
    private KuduQueryRunner kuduQueryRunner;

    public TestKuduIntegrationSmokeTest() {
        super(() -> createKuduQueryRunner(ORDERS));
    }

    @BeforeClass
    public void setUp()
            throws Exception {

        kuduQueryRunner = (KuduQueryRunner) getQueryRunner();
    }

    /**
     * Overrides original implementation because of usage of 'extra' column.
     * @throws Exception
     */
    @Test
    @Override
    public void testDescribeTable() throws Exception {
        MaterializedResult actualColumns = this.computeActual("DESC ORDERS").toJdbcTypes();
        MaterializedResult.Builder builder = MaterializedResult.resultBuilder(this.getQueryRunner().getDefaultSession(), VarcharType.VARCHAR, VarcharType.VARCHAR, VarcharType.VARCHAR, VarcharType.VARCHAR);
        for (MaterializedRow row: actualColumns.getMaterializedRows()) {
            builder.row(row.getField(0), row.getField(1), "", "");
        }
        MaterializedResult filteredActual = builder.build();
        builder = MaterializedResult.resultBuilder(this.getQueryRunner().getDefaultSession(), VarcharType.VARCHAR, VarcharType.VARCHAR, VarcharType.VARCHAR, VarcharType.VARCHAR);
        MaterializedResult expectedColumns = builder
                .row("orderkey", "bigint", "", "")
                .row("custkey", "bigint", "", "")
                .row("orderstatus", "varchar", "", "")
                .row("totalprice", "double", "", "")
                .row("orderdate", "varchar", "", "")
                .row("orderpriority", "varchar", "", "")
                .row("clerk", "varchar", "", "")
                .row("shippriority", "integer", "", "")
                .row("comment", "varchar", "", "").build();
        Assert.assertEquals(filteredActual, expectedColumns, String.format("%s != %s", filteredActual, expectedColumns));
    }


    @AfterClass(alwaysRun = true)
    public final void destroy() {
        kuduQueryRunner.shutdown();
        kuduQueryRunner = null;
    }
}

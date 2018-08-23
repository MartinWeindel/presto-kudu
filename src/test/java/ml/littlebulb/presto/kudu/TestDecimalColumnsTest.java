package ml.littlebulb.presto.kudu;

import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

class TestDec {
    final int precision;
    final int scale;

    TestDec(int precision, int scale) {
        this.precision = precision;
        this.scale = scale;
    }
}

public class TestDecimalColumnsTest extends AbstractTestQueryFramework {
    private QueryRunner queryRunner;

    static final TestDec[] testDecList = {
            new TestDec(10,0),
            new TestDec(15,4),
            new TestDec(18,6),
            new TestDec(18,7),
            new TestDec(19,8),
            new TestDec(24,14),
            new TestDec(38,20),
            new TestDec(38, 28),
    };


    public TestDecimalColumnsTest() throws Exception {
        super(() -> KuduQueryRunnerFactory.createKuduQueryRunner("decimal"));
    }

    @Test
    public void testCreateTableWithDecimalColumn() {
        for (TestDec dec : testDecList) {
            doTestCreateTableWithDecimalColumn(dec);
        }
    }


    public void doTestCreateTableWithDecimalColumn(TestDec dec) {
        String dropTable = "DROP TABLE IF EXISTS test_dec";
        String createTable = "CREATE TABLE test_dec (\n";
        createTable += "  id INT,\n";
        createTable += "  dec DECIMAL(" + dec.precision + "," + dec.scale + ")\n";
        createTable += ") WITH (\n" +
                        " column_design = '{\"id\": {\"key\": true}}',\n" +
                        " partition_design = '{\"hash\":[{\"columns\":[\"id\"], \"buckets\": 2}]}',\n" +
                        " num_replicas = 1\n" +
                        ")";

        queryRunner.execute(dropTable);
        queryRunner.execute(createTable);

        String fullPrecisionValue = "1234567890.1234567890123456789012345678";
        int maxScale = dec.precision - 10;
        int valuePrecision = dec.precision - maxScale + Math.min(maxScale, dec.scale);
        String insertValue = fullPrecisionValue.substring(0, valuePrecision + 1);
        queryRunner.execute("INSERT INTO test_dec VALUES(1, DECIMAL '" + insertValue + "')");

        MaterializedResult result = queryRunner.execute("SELECT id, CAST((dec - (DECIMAL '" + insertValue + "')) as DOUBLE) FROM test_dec");
        Assert.assertEquals(result.getRowCount(), 1);
        Object obj = result.getMaterializedRows().get(0).getField(1);
        Assert.assertTrue(obj instanceof Double);
        Double actual = (Double) obj;
        Assert.assertEquals(0, actual, 0.3 * Math.pow(0.1, dec.scale), "p=" + dec.precision + ",s=" + dec.scale + " => " + actual + ",insert = " + insertValue);
    }

    @BeforeClass
    public void setUp() {
        queryRunner = getQueryRunner();
    }

    @AfterClass(alwaysRun = true)
    public final void destroy() {
        if (queryRunner != null) {
            queryRunner.close();
            queryRunner = null;
        }
    }
}

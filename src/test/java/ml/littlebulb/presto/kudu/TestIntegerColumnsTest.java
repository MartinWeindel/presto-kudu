package ml.littlebulb.presto.kudu;

import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

class TestInt {
    final String type;
    final int bits;

    TestInt(String type, int bits) {
        this.type = type;
        this.bits = bits;
    }
}

public class TestIntegerColumnsTest extends AbstractTestQueryFramework {
    private QueryRunner queryRunner;

    static final TestInt[] testList = {
            new TestInt("TINYINT", 8),
            new TestInt("SMALLINT", 16),
            new TestInt("INTEGER", 32),
            new TestInt("BIGINT", 64),
    };


    public TestIntegerColumnsTest() throws Exception {
        super(() -> KuduQueryRunnerFactory.createKuduQueryRunner("test_integer"));
    }

    @Test
    public void testCreateTableWithIntegerColumn() {
        for (TestInt test : testList) {
            doTestCreateTableWithIntegerColumn(test);
        }
    }


    public void doTestCreateTableWithIntegerColumn(TestInt test) {
        String dropTable = "DROP TABLE IF EXISTS test_int";
        String createTable = "CREATE TABLE test_int (\n";
        createTable += "  id INT,\n";
        createTable += "  intcol " + test.type + "\n";
        createTable += ") WITH (\n" +
                        " column_design = '{\"id\": {\"key\": true}}',\n" +
                        " partition_design = '{\"hash\":[{\"columns\":[\"id\"], \"buckets\": 2}]}',\n" +
                        " num_replicas = 1\n" +
                        ")";

        queryRunner.execute(dropTable);
        queryRunner.execute(createTable);

        long maxValue = Long.MAX_VALUE;
        long casted = maxValue >> (64 - test.bits);
        queryRunner.execute("INSERT INTO test_int VALUES(1, CAST("  + casted + " AS " + test.type + "))");

        MaterializedResult result = queryRunner.execute("SELECT id, intcol FROM test_int");
        Assert.assertEquals(result.getRowCount(), 1);
        Object obj = result.getMaterializedRows().get(0).getField(1);
        switch (test.bits) {
            case 64:
                Assert.assertTrue(obj instanceof Long);
                Assert.assertEquals(((Long) obj).longValue(), casted);
                break;
            case 32:
                Assert.assertTrue(obj instanceof Integer);
                Assert.assertEquals(((Integer) obj).longValue(), casted);
                break;
            case 16:
                Assert.assertTrue(obj instanceof Short);
                Assert.assertEquals(((Short) obj).longValue(), casted);
                break;
            case 8:
                Assert.assertTrue(obj instanceof Byte);
                Assert.assertEquals(((Byte) obj).longValue(), casted);
                break;
            default:
                Assert.fail("Unexpected bits: " + test.bits);
                break;
        }
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

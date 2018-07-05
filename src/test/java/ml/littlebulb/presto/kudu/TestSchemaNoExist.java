package ml.littlebulb.presto.kudu;

import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.testng.Assert;
import org.testng.annotations.*;
import org.testng.annotations.Test;
public class TestSchemaNoExist extends AbstractTestQueryFramework {
    private TestingKuduQueryRunner kuduQueryRunner;


    public TestSchemaNoExist() {
        super(TestingKuduQueryRunner::createKuduQueryRunner);
    }

    private static final String SCHEMA_NAME = "test_presto_schema";

    private static final String CREATE_SCHEMA = "create schema if not exists kudu." + SCHEMA_NAME;
    private static final String DROP_SCHEMA = "drop schema if exists kudu." + SCHEMA_NAME;


    private static final String CREATE_TABLE = "create table if not exists kudu." + SCHEMA_NAME + ".test_presto_table " +
            "(user_id int, user_name varchar) " +
            "with(column_design = '{\"user_id\": {\"key\": true}}'," +
            "partition_design = '{\"hash\":[{\"columns\":[\"user_id\"], \"buckets\": 2}]}'," +
            "num_replicas = 1)";
    private static final String DROP_TABLE = "drop table if exists kudu." + SCHEMA_NAME + ".test_presto_table";



    @Test
    public void testCreateTableWithoutSchema() {

        String errorMessage="";
        try {
            kuduQueryRunner.execute(CREATE_TABLE);
        } catch (Exception e) {
            errorMessage=e.getMessage();
        }
        Assert.assertTrue(errorMessage.equals("schema is not exists:" + SCHEMA_NAME));

    }

    @AfterMethod
    public void afterTest() {
        kuduQueryRunner.execute(DROP_TABLE);
        kuduQueryRunner.execute(DROP_SCHEMA);
    }


    @BeforeClass
    public void setUp() {
        kuduQueryRunner = (TestingKuduQueryRunner) getQueryRunner();
    }

    @AfterClass(alwaysRun = true)
    public final void destroy() {
        kuduQueryRunner.execute(DROP_TABLE);
        kuduQueryRunner.execute(DROP_SCHEMA);
        kuduQueryRunner.shutdown();
        kuduQueryRunner = null;
    }
}

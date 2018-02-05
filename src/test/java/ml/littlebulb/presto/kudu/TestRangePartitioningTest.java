package ml.littlebulb.presto.kudu;

import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Arrays;

class TestRanges {
    String[] types;
    String range1, range2, range3, range4;

    TestRanges(String type, String range1, String range2, String range3, String range4) {
        this(new String[]{type}, range1, range2, range3, range4);
    }

    TestRanges(String[] types, String range1, String range2, String range3, String range4) {
        this.types = types;
        this.range1 = range1;
        this.range2 = range2;
        this.range3 = range3;
        this.range4 = range4;
    }
}

public class TestRangePartitioningTest extends AbstractTestQueryFramework {
    private TestingKuduQueryRunner kuduQueryRunner;

    static final TestRanges[] testRangesList = {
            new TestRanges("varchar",
                    "{\"lower\": null, \"upper\": \"D\"}",
                    "{\"lower\": \"D\", \"upper\": \"M\"}",
                    "{\"lower\": \"M\", \"upper\": \"S\"}",
                    "{\"lower\": \"S\", \"upper\": null}"),
            new TestRanges("timestamp",
                    "{\"lower\": null, \"upper\": \"2017-01-01T02:03:04.567\"}",
                    "{\"lower\": \"2017-01-01 02:03:04.567\", \"upper\": \"2017-02-01 12:34\"}",
                    "{\"lower\": \"2017-02-01 12:34\", \"upper\": \"2017-03-01\"}",
                    "{\"lower\": \"2017-03-01\", \"upper\": null}"),
            new TestRanges("tinyint",
                    "{\"lower\": null, \"upper\": -100}",
                    "{\"lower\": \"-100\", \"upper\": 0}",
                    "{\"lower\": 0, \"upper\": 100}",
                    "{\"lower\": 100, \"upper\": 200}"),
            new TestRanges("smallint",
                    "{\"lower\": null, \"upper\": -100}",
                    "{\"lower\": \"-100\", \"upper\": 0}",
                    "{\"lower\": 0, \"upper\": 100}",
                    "{\"lower\": 100, \"upper\": 200}"),
            new TestRanges("integer",
                    "{\"lower\": null, \"upper\": -1000000}",
                    "{\"lower\": \"-1000000\", \"upper\": 0}",
                    "{\"lower\": 0, \"upper\": 100}",
                    "{\"lower\": 100, \"upper\": 200}"),
            new TestRanges("bigint",
                    "{\"lower\": null, \"upper\": \"-123456789012345\"}",
                    "{\"lower\": \"-123456789012345\", \"upper\": 0}",
                    "{\"lower\": 0, \"upper\": 100}",
                    "{\"lower\": 100, \"upper\": 200}"),
            new TestRanges("varbinary",
                    "{\"lower\": null, \"upper\": \"YWI=\"}",
                    "{\"lower\": \"YWI=\", \"upper\": \"ZA==\"}",
                    "{\"lower\": \"ZA==\", \"upper\": \"bW1t\"}",
                    "{\"lower\": \"bW1t\", \"upper\": \"eg==\"}"),
            new TestRanges(new String[] {"smallint","varchar"},
                    "{\"lower\": null, \"upper\": [1, \"M\"]}",
                    "{\"lower\": [1, \"M\"], \"upper\": [1, \"T\"]}",
                    "{\"lower\": [1, \"T\"], \"upper\": [2, \"Z\"]}",
                    "{\"lower\": [2, \"Z\"], \"upper\": null}"),
    };

    public TestRangePartitioningTest() throws Exception {
        super(() -> TestingKuduQueryRunner.createKuduQueryRunner());
    }

    @Test
    public void testCreateAndChangeTableWithRangePartition() {
        for (TestRanges ranges : testRangesList) {
            doTestCreateAndChangeTableWithRangePartition(ranges);
        }
    }


    public void doTestCreateAndChangeTableWithRangePartition(TestRanges ranges) {
        String[] types = ranges.types;
        String name = String.join("_", ranges.types);
        String createTable = "CREATE TABLE range_partitioning_" + name + " (\n";
        String columnDesign = "";
        String partitionDesign = "";
        for (int i = 0; i < types.length; i++) {
            String type = types[i];
            String columnName = "key" + i;
            createTable += "  " + columnName + " " + type + ",\n";
            if (i == 0) {
                columnDesign += "{";
                partitionDesign += "[";
            } else {
                columnDesign += ",";
                partitionDesign += ",";
            }
            columnDesign += "\"" + columnName + "\": {\"key\": true}";
            partitionDesign += "\"" + columnName + "\"";
        }
        columnDesign += "}";
        partitionDesign += "]";

        createTable +=
                "  value varchar\n" +
                ") WITH (\n" +
                " column_design = '" + columnDesign + "',\n" +
                " partition_design = '{\"range\": {\"columns\":" + partitionDesign + "}}',\n" +
                " range_partitions = '[" + ranges.range1 + "," + ranges.range2 + "]',\n" +
                " num_replicas = 1\n" +
                ")";
        kuduQueryRunner.execute(createTable);
    }

    @BeforeClass
    public void setUp() {
        kuduQueryRunner = (TestingKuduQueryRunner) getQueryRunner();
    }

    @AfterClass(alwaysRun = true)
    public final void destroy() {
        kuduQueryRunner.shutdown();
        kuduQueryRunner = null;
    }
}

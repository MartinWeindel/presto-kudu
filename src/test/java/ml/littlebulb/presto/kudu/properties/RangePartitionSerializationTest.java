package ml.littlebulb.presto.kudu.properties;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;

public class RangePartitionSerializationTest {
    private String[] testInputs = new String[]{
            "{\"lower\":1,\"upper\":null}",
            "{\"lower\":12345678901234567890,\"upper\":1.234567890123457E-13}",
            "{\"lower\":\"abc\",\"upper\":\"abf\"}",
            "{\"lower\":false,\"upper\":true}",
            "{\"lower\":\"ABCD\",\"upper\":\"ABCDEF\"}",
            "{\"lower\":[\"ABCD\",1,0],\"upper\":[\"ABCD\",13,0]}",
    };

    @Test
    public void testDeserializationSerialization() throws IOException {
        ObjectMapper mapper = new ObjectMapper();

        for (String input : testInputs) {
            RangePartition partition = mapper.readValue(input, RangePartition.class);

            String serialized = mapper.writeValueAsString(partition);
            Assert.assertEquals(serialized, input);
        }
    }
}

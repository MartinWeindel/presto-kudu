package ml.littlebulb.presto.kudu.properties;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

public class RangeBoundValueDeserializer extends JsonDeserializer {
    @Override
    public RangeBoundValue deserialize(JsonParser jp, DeserializationContext ctxt)
            throws IOException {
        JsonNode node = jp.getCodec().readTree(jp);

        if (node.isNull()) {
            return null;
        } else {
            RangeBoundValue value = new RangeBoundValue();
            if (node.isArray()) {
                ArrayList<Object> list = new ArrayList<>();
                Iterator<JsonNode> iter = node.elements();
                while (iter.hasNext()) {
                    Object v = toValue(iter.next());
                    list.add(v);
                }
                value.setValues(ImmutableList.copyOf(list));
            } else {
                Object v = toValue(node);
                value.setValues(ImmutableList.of(v));
            }
            return value;
        }
    }

    private Object toValue(JsonNode node) throws IOException {
        if (node.isTextual()) {
            return node.asText();
        } else if (node.isNumber()) {
            return node.numberValue();
        } else if (node.isBoolean()) {
            return node.asBoolean();
        } else if (node.isBinary()) {
            return node.binaryValue();
        } else {
            throw new IllegalStateException("Unexpected range bound value: " + node);
        }
    }
}

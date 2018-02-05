package ml.littlebulb.presto.kudu.properties;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.weakref.jmx.internal.guava.collect.ImmutableList;

import java.util.List;

@JsonDeserialize(using = RangeBoundValueDeserializer.class)
@JsonSerialize(using = RangeBoundValueSerializer.class)
public class RangeBoundValue {
    private List<Object> values;

    public List<Object> getValues() {
        return values;
    }

    public void setValues(List<Object> values) {
        this.values = ImmutableList.copyOf(values);
    }
}

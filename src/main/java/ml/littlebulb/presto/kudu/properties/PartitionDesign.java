package ml.littlebulb.presto.kudu.properties;

import java.util.List;

public class PartitionDesign {
    private List<HashPartitionDefinition> hash;
    private RangePartitionDefinition range;

    public List<HashPartitionDefinition> getHash() {
        return hash;
    }

    public void setHash(List<HashPartitionDefinition> hash) {
        this.hash = hash;
    }

    public RangePartitionDefinition getRange() {
        return range;
    }

    public void setRange(RangePartitionDefinition range) {
        this.range = range;
    }
}

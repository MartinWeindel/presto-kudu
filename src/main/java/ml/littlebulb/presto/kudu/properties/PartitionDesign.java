package ml.littlebulb.presto.kudu.properties;

import java.util.List;

public class PartitionDesign {
    private List<HashPartition> hash;
    private RangePartition range;

    public List<HashPartition> getHash() {
        return hash;
    }

    public void setHash(List<HashPartition> hash) {
        this.hash = hash;
    }

    public RangePartition getRange() {
        return range;
    }

    public void setRange(RangePartition range) {
        this.range = range;
    }
}

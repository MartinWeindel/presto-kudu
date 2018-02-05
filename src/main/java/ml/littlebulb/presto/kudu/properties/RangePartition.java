package ml.littlebulb.presto.kudu.properties;

public class RangePartition {
    private RangeBoundValue lower;
    private RangeBoundValue upper;

    public RangeBoundValue getLower() {
        return lower;
    }

    public void setLower(RangeBoundValue lower) {
        this.lower = lower;
    }

    public RangeBoundValue getUpper() {
        return upper;
    }

    public void setUpper(RangeBoundValue upper) {
        this.upper = upper;
    }
}

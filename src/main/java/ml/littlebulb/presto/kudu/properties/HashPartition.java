package ml.littlebulb.presto.kudu.properties;

import java.util.List;

public class HashPartition {
    private List<String> columns;
    private int buckets;

    public List<String> getColumns() {
        return columns;
    }

    public void setColumns(List<String> columns) {
        this.columns = columns;
    }

    public int getBuckets() {
        return buckets;
    }

    public void setBuckets(int buckets) {
        this.buckets = buckets;
    }
}

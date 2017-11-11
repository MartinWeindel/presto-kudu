package ml.littlebulb.presto.kudu.properties;

public class ColumnDesign {
    public static final ColumnDesign DEFAULT;

    static {
        ColumnDesign design = new ColumnDesign();
        design.setNullable(true);
        DEFAULT = design;
    }

    private boolean key;
    private boolean nullable;
    private String encoding;
    private String compression;

    public boolean isKey() {
        return key;
    }

    public void setKey(boolean key) {
        this.key = key;
    }

    public String getEncoding() {
        return encoding;
    }

    public void setEncoding(String encoding) {
        this.encoding = encoding;
    }

    public String getCompression() {
        return compression;
    }

    public void setCompression(String compression) {
        this.compression = compression;
    }

    public boolean isNullable() {
        return nullable;
    }

    public void setNullable(boolean nullable) {
        this.nullable = nullable;
    }
}

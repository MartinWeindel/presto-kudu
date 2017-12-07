package ml.littlebulb.presto.kudu;

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.SchemaTableName;
import org.apache.kudu.Schema;
import org.apache.kudu.client.KuduScanner;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;

import java.util.List;
import java.util.Map;

public interface KuduClientSession {
    List<String> listSchemaNames();

    List<SchemaTableName> listTables(String schemaNameOrNull);

    Schema getTableSchema(KuduTableHandle tableName);

    boolean tableExists(SchemaTableName schemaTableName);

    Map<String, Object> getTableProperties(KuduTableHandle tableName);

    List<KuduSplit> buildKuduSplits(KuduTableLayoutHandle layoutHandle);

    KuduScanner createScanner(KuduSplit kuduSplit);

    KuduTable openTable(SchemaTableName schemaTableName);

    KuduSession newSession();

    void createSchema(String schemaName);

    void dropSchema(String schemaName);

    void dropTable(SchemaTableName schemaTableName);

    void renameTable(SchemaTableName schemaTableName, SchemaTableName newSchemaTableName);

    KuduTable createTable(ConnectorTableMetadata tableMetadata, boolean ignoreExisting);

    void addColumn(SchemaTableName schemaTableName, ColumnMetadata column);

    void dropColumn(SchemaTableName schemaTableName, String name);

    void renameColumn(SchemaTableName schemaTableName, String oldName, String newName);
}

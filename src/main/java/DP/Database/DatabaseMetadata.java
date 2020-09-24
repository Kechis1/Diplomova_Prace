package DP.Database;

import java.util.List;


public class DatabaseMetadata {
    private String tableCatalog;
    private String tableSchema;
    private List<DatabaseTable> tables;

    public DatabaseMetadata(String tableCatalog, String tableSchema, List<DatabaseTable> tables) {
        this.tableCatalog = tableCatalog;
        this.tableSchema = tableSchema;
        this.tables = tables;
    }

    public String getTableCatalog() {
        return tableCatalog;
    }

    public void setTableCatalog(String tableCatalog) {
        this.tableCatalog = tableCatalog;
    }

    public String getTableSchema() {
        return tableSchema;
    }

    public void setTableSchema(String tableSchema) {
        this.tableSchema = tableSchema;
    }

    public List<DatabaseTable> getTables() {
        return tables;
    }

    public void setTables(List<DatabaseTable> tables) {
        this.tables = tables;
    }
}

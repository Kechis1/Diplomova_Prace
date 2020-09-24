package DP.Database;

import java.util.List;

public class DatabaseTable {
    private String tableName;
    private List<String> columns;
    private String primaryKeys;

    public DatabaseTable(String tableName, List<String> columns, String primaryKeys) {
        this.tableName = tableName;
        this.columns = columns;
        this.primaryKeys = primaryKeys;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public List<String> getColumns() {
        return columns;
    }

    public void setColumns(List<String> columns) {
        this.columns = columns;
    }

    public String getPrimaryKeys() {
        return primaryKeys;
    }

    public void setPrimaryKeys(String primaryKeys) {
        this.primaryKeys = primaryKeys;
    }
}

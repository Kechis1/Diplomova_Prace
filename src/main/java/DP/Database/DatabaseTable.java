package DP.Database;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class DatabaseTable {
    private String tableName;
    private String tableAlias;
    private List<String> columns;
    private List<String> primaryKeys;

    public DatabaseTable(String tableName, List<String> columns, List<String> primaryKeys, String tableAlias) {
        this.tableName = tableName;
        this.columns = columns;
        this.primaryKeys = primaryKeys;
        this.tableAlias = tableAlias;
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

    public List<String> getPrimaryKeys() {
        return primaryKeys;
    }

    public void setPrimaryKeys(List<String> primaryKeys) {
        this.primaryKeys = primaryKeys;
    }

    public String getTableAlias() {
        return tableAlias;
    }

    public void setTableAlias(String tableAlias) {
        this.tableAlias = tableAlias;
    }

    public Collection<? extends ColumnItem> getColumnItems() {
        List<ColumnItem> items = new ArrayList<>();
        for (String item : getColumns()) {
            items.add(new ColumnItem(null, null, new TableItem(getTableName(), getTableAlias()), item));
        }
        return items;
    }

    @Override
    public String toString() {
        return "DatabaseTable{" +
                "tableName='" + tableName + '\'' +
                ", tableAlias='" + tableAlias + '\'' +
                ", columns=" + columns +
                ", primaryKeys=" + primaryKeys +
                '}';
    }
}

package DP.Database;

import DP.antlr4.tsql.parser.TSqlParser;

import javax.xml.crypto.Data;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

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

    public DatabaseTable() {}

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

    public static DatabaseTable create(DatabaseMetadata metadata, TSqlParser.Table_source_itemContext ctx) {
        DatabaseTable newItem = metadata.findTable(ctx.table_name_with_hint().table_name().table.getText());
        newItem.setTableAlias(ctx.as_table_alias() != null
                ? ctx.as_table_alias().getText()
                : ctx.table_name_with_hint().table_name().table.getText());
        return newItem;
    }

    public static DatabaseTable create(DatabaseMetadata metadata, String name, String alias) {
        DatabaseTable newItem;
        if (name != null) {
            newItem = metadata.findTable(name);
        } else if (alias != null) {
            newItem = metadata.findTable(alias);
        } else {
            newItem = new DatabaseTable();
        }
        newItem.setTableAlias(alias);
        return newItem;
    }

    public static List<DatabaseTable> difference(List<DatabaseTable> allItems, List<DatabaseTable> filteredItems) {
        List<DatabaseTable> newItems = new ArrayList<>(allItems);

        for (DatabaseTable filteredItem: filteredItems) {
            newItems.removeIf(item -> item.equals(filteredItem));
        }

        return newItems;
    }

    public List<ColumnItem> getColumnItems() {
        List<ColumnItem> items = new ArrayList<>();
        for (String item : getColumns()) {
            items.add(new ColumnItem(null, null, this, item));
        }
        return items;
    }

    public boolean columnExists(ColumnItem columnItem) {
        List<ColumnItem> allColumns = getColumnItems();
        return ColumnItem.exists(allColumns, columnItem);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DatabaseTable table = (DatabaseTable) o;
        return Objects.equals(tableName, table.tableName) &&
                Objects.equals(tableAlias, table.tableAlias);
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

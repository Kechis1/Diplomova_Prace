package DP.Database;

import DP.Exceptions.UnnecessaryStatementException;
import DP.antlr4.tsql.parser.TSqlParser;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class DatabaseTable {
    private String tableName;
    private String tableAlias;
    private List<ColumnItem> columns;
    private List<String> primaryKeys;
    private List<ForeignKey> foreignKeys;
    private boolean isColumnsTableSet = false;

    public DatabaseTable(String tableName, List<ColumnItem> columns, List<String> primaryKeys, List<ForeignKey> foreignKeys, String tableAlias) {
        this.tableName = tableName;
        this.columns = columns;
        this.primaryKeys = primaryKeys;
        this.foreignKeys = foreignKeys;
        this.tableAlias = tableAlias;
    }

    public DatabaseTable() {}

    public static boolean redundantJoinExists(String typeOfJoin, List<DatabaseTable> joins, String tableAlias, DatabaseTable databaseTable, List<ColumnItem> allColumnsInSelect, boolean checkNullable, List<ConditionItem> newConditions, boolean checkBothSides) {
        for (DatabaseTable table : joins) {
            boolean found = false;
            for (ColumnItem cItem : allColumnsInSelect) {
                if ((cItem.getName().equals("*") && (cItem.getTable().getTableAlias() == null || !cItem.getTable().getTableAlias().equals(tableAlias == null ? table.getTableAlias() : tableAlias)))
                        || (!cItem.getName().equals("*") && !(databaseTable == null ? table : databaseTable).columnExists(cItem))) {
                    found = true;
                    break;
                }
            }
            if ((!checkNullable && !found) || (checkNullable && !found && !ConditionItem.isConditionColumnNullable(newConditions, table, checkBothSides))) {
                System.out.println(UnnecessaryStatementException.messageUnnecessaryStatement + " " + typeOfJoin + " JOIN");
                return true;
            }
        }
        return false;
    }

    public List<ForeignKey> getForeignKeys() {
        return foreignKeys;
    }

    public void setForeignKeys(List<ForeignKey> foreignKeys) {
        this.foreignKeys = foreignKeys;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public List<ColumnItem> getColumns() {
        if (!isColumnsTableSet) {
            for (ColumnItem currItem : columns) {
                currItem.setTable(this);
            }
            isColumnsTableSet = true;
        }
        return columns;
    }

    public void setColumns(List<ColumnItem> columns) {
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
        DatabaseTable newItem = metadata.findTable(ctx.table_name_with_hint().table_name().table.getText(), null);
        newItem.setTableAlias(ctx.as_table_alias() != null
                ? ctx.as_table_alias().getText()
                : ctx.table_name_with_hint().table_name().table.getText());
        return newItem;
    }

    public static DatabaseTable create(DatabaseMetadata metadata, String name, String alias) {
        DatabaseTable newItem;
        if (name != null) {
            newItem = metadata.findTable(name, null);
        } else if (alias != null) {
            newItem = metadata.findTable(null, alias);
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

    public boolean columnExists(ColumnItem columnItem) {
        return ColumnItem.exists(getColumns(), columnItem);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DatabaseTable table = (DatabaseTable) o;
        return Objects.equals(tableName, table.tableName) &&
                Objects.equals(tableAlias, table.tableAlias);
    }

    public ColumnItem findColumn(String name) {
        for (ColumnItem item : columns) {
            if (item.getName().equalsIgnoreCase(name)) {
                return item;
            }
        }
        return null;
    }

    @Override
    public String toString() {
        return "DatabaseTable{" +
                "tableName='" + tableName + '\'' +
                ", tableAlias='" + tableAlias + '\'' +
                ", columns=" + columns +
                ", primaryKeys=" + primaryKeys +
                ", foreignKeys=" + foreignKeys +
                ", isColumnsTableSet=" + isColumnsTableSet +
                '}';
    }
}

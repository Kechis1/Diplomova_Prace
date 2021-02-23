package DP.Database;

import DP.Transformations.JoinTableTransformation;
import DP.Transformations.Query;
import DP.Transformations.Transformation;
import DP.Exceptions.UnnecessaryStatementException;
import DP.antlr4.tsql.parser.TSqlParser;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class DatabaseTable {
    private String queryName;
    private String tableName;
    private String tableAlias;
    private List<ColumnItem> columns;
    private List<String> primaryKeys;
    private List<ForeignKey> foreignKeys;
    private int recordsCount;
    private boolean isColumnsTableSet = false;
    private int fromTableStartAt;
    private int fromTableStopAt;

    public DatabaseTable(String queryName, String tableName, List<ColumnItem> columns, List<String> primaryKeys, List<ForeignKey> foreignKeys, String tableAlias, int recordsCount) {
        this.queryName = queryName;
        this.tableName = tableName;
        this.columns = columns;
        this.primaryKeys = primaryKeys;
        this.foreignKeys = foreignKeys;
        this.tableAlias = tableAlias;
        this.recordsCount = recordsCount;
    }

    public DatabaseTable() {
    }

    public String getQueryName() {
        return queryName;
    }

    public void setQueryName(String queryName) {
        this.queryName = queryName;
    }

    public boolean isColumnsTableSet() {
        return isColumnsTableSet;
    }

    public void setColumnsTableSet(boolean columnsTableSet) {
        isColumnsTableSet = columnsTableSet;
    }

    public int getFromTableStartAt() {
        return fromTableStartAt;
    }

    public void setFromTableStartAt(int fromTableStartAt) {
        this.fromTableStartAt = fromTableStartAt;
    }

    public int getFromTableStopAt() {
        return fromTableStopAt;
    }

    public void setFromTableStopAt(int fromTableStopAt) {
        this.fromTableStopAt = fromTableStopAt;
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

    public int getRecordsCount() {
        return recordsCount;
    }

    public void setRecordsCount(int recordsCount) {
        this.recordsCount = recordsCount;
    }

    public static int exists(DatabaseMetadata metadata, String name, String alias) {
        if (name != null) {
            return metadata.tableExists(name, null);
        } else if (alias != null) {
            return metadata.tableExists(null, alias);
        }
        return -1;
    }

    public static DatabaseTable create(DatabaseMetadata metadata, TSqlParser.Table_source_itemContext ctx) {
        DatabaseTable newItem;
        if (ctx.table_name_with_hint() != null) {
            newItem = metadata.findTable(ctx.table_name_with_hint().table_name().table.getText(), null);
        } else {
            newItem = new DatabaseTable();
        }
        if (ctx.as_table_alias() != null && ctx.table_name_with_hint() != null) {
            newItem.setQueryName(ctx.table_name_with_hint().getText() + " " + ctx.as_table_alias().getText());
        } else if (ctx.as_table_alias() != null) {
            newItem.setQueryName(ctx.as_table_alias().getText());
        } else {
            newItem.setQueryName(ctx.getText());
        }
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
        } else if (metadata.getTables().size() == 1) {
            newItem = metadata.getTables().get(0);
        } else {
            newItem = new DatabaseTable();
        }
        newItem.setTableAlias(alias);
        return newItem;
    }

    public static List<DatabaseTable> difference(List<DatabaseTable> allItems, List<DatabaseTable> filteredItems) {
        List<DatabaseTable> newItems = new ArrayList<>(allItems);

        for (DatabaseTable filteredItem : filteredItems) {
            newItems.removeIf(item -> item.equals(filteredItem));
        }

        return newItems;
    }

    public static boolean redundantJoinExists(Query query, String typeOfJoin, List<JoinTable> joins, String tableAlias, DatabaseTable databaseTable, List<ColumnItem> allColumnsInSelect, boolean checkNullable, List<ConditionItem> newConditions, boolean checkBothSides, DatabaseTable fromTable) {
        for (JoinTable joinTable : joins) {
            boolean found = false;
            DatabaseTable table = joinTable.getDatabaseTable();
            for (ColumnItem cItem : allColumnsInSelect) {
                if ((cItem.getName().equals("*") && (cItem.getTable().getTableAlias() == null || !cItem.getTable().getTableAlias().equals(tableAlias == null ? table.getTableAlias() : tableAlias)))
                        || (!cItem.getName().equals("*") && !(databaseTable == null ? table : databaseTable).columnExists(cItem))) {
                    found = true;
                    break;
                }
            }
            if ((!checkNullable && !found) || (checkNullable && !found && !ConditionItem.isConditionColumnNullable(newConditions, table, checkBothSides))) {
                String currentQuery;
                if (typeOfJoin.equals("RIGHT") || typeOfJoin.equals("INNER")) {
                    currentQuery = (query.getCurrentQuery().substring(0, fromTable.getFromTableStartAt()) + joinTable.getDatabaseTable().getQueryName() +
                            query.getCurrentQuery().substring(fromTable.getFromTableStopAt() + 1, joinTable.getStartAt()) + query.getCurrentQuery().substring(joinTable.getStopAt() + 1)).trim();
                } else {
                    currentQuery = (query.getCurrentQuery().substring(0, joinTable.getStartAt()) + query.getCurrentQuery().substring(joinTable.getStopAt() + 1)).trim();
                }

                query.addTransformation(new Transformation(query.getCurrentQuery(),
                        currentQuery,
                        UnnecessaryStatementException.messageUnnecessaryStatement + " " + typeOfJoin + " JOIN",
                        JoinTableTransformation.action,
                        true
                ));
                return true;
            }
        }
        return false;
    }

    public boolean columnExists(ColumnItem columnItem) {
        return ColumnItem.exists(getColumns(), columnItem);
    }

    public ColumnItem findColumn(String name) {
        for (ColumnItem item : columns) {
            if (item.getName().equalsIgnoreCase(name)) {
                item.setTable(this);
                return item;
            }
        }
        return null;
    }

    public boolean isEmpty() {
        return getRecordsCount() == 0;
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
                ", foreignKeys=" + foreignKeys +
                ", isColumnsTableSet=" + isColumnsTableSet +
                '}';
    }

}

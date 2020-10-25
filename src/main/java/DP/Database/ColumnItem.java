package DP.Database;

import DP.antlr4.tsql.parser.TSqlParser;

import java.util.List;

public class ColumnItem {
    String database;
    String schema;
    DatabaseTable table;
    String name;
    boolean isNullable = true;

    public ColumnItem(String database, String schema, DatabaseTable table, String name, boolean isNullable) {
        this.database = database;
        this.schema = schema;
        this.table = table;
        this.name = name;
        this.isNullable = isNullable;
    }

    public ColumnItem(String database, String schema, DatabaseTable table, String name) {
        this.database = database;
        this.schema = schema;
        this.table = table;
        this.name = name;
    }

    public static ColumnItem create(DatabaseMetadata metadata, TSqlParser.Search_conditionContext ctx, int index) {
        if (ctx.search_condition_and().get(0).search_condition_not().get(0).predicate().expression().get(index).full_column_name().table_name() != null) {
            DatabaseTable table = DatabaseTable.create(metadata,
                    null,
                    ctx.search_condition_and().get(0).search_condition_not().get(0).predicate().expression().get(index).full_column_name().table_name().table != null
                            ? ctx.search_condition_and().get(0).search_condition_not().get(0).predicate().expression().get(index).full_column_name().table_name().table.getText()
                            : null);

            return new ColumnItem(
                    ctx.search_condition_and().get(0).search_condition_not().get(0).predicate().expression().get(index).full_column_name().table_name().database != null
                            ? ctx.search_condition_and().get(0).search_condition_not().get(0).predicate().expression().get(index).full_column_name().table_name().database.getText()
                            : null,
                    ctx.search_condition_and().get(0).search_condition_not().get(0).predicate().expression().get(index).full_column_name().table_name().schema != null
                            ? ctx.search_condition_and().get(0).search_condition_not().get(0).predicate().expression().get(index).full_column_name().table_name().schema.getText()
                            : null,
                    table,
                    ctx.search_condition_and().get(0).search_condition_not().get(0).predicate().expression().get(index).full_column_name().column_name.getText()
            );
        }

        return new ColumnItem(
                null,
                null,
                null,
                ctx.search_condition_and().get(0).search_condition_not().get(0).predicate().expression().get(index).full_column_name().column_name.getText()
        );
    }

    public static boolean exists(List<ColumnItem> allColumns, ColumnItem columnItem) {
        for (ColumnItem currentItem : allColumns) {
            if (!currentItem.getName().equals(columnItem.getName())) {
                continue;
            }
            if (columnItem.getTable().getTableAlias() == null || currentItem.getTable().getTableAlias().equals(columnItem.getTable().getTableAlias())) {
                return true;
            }
        }
        return false;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public DatabaseTable getTable() {
        return table;
    }

    public void setTable(DatabaseTable table) {
        this.table = table;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public boolean isNullable() {
        return isNullable;
    }

    public void setNullable(boolean nullable) {
        isNullable = nullable;
    }

    @Override
    public String toString() {
        String tableString = "";
        if (table != null) {
            tableString += "{tableName=" + table.getTableName() + ", tableAlias=" + table.getTableAlias() + "}";
        } else {
            tableString = "null";
        }
        return "ColumnItem{" +
                "database='" + database + '\'' +
                ", schema='" + schema + '\'' +
                ", table=" + tableString +
                ", name='" + name + '\'' +
                ", isNullable=" + isNullable +
                '}';
    }
}

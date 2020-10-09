package DP.Database;

import DP.antlr4.tsql.parser.TSqlParser;

public class ColumnItem {
    String database;
    String schema;
    TableItem table;
    String name;

    public ColumnItem(String database, String schema, TableItem table, String name) {
        this.database = database;
        this.schema = schema;
        this.table = table;
        this.name = name;
    }

    public static ColumnItem create(TSqlParser.Search_conditionContext ctx, int index) {
        if (ctx.search_condition_and().get(0).search_condition_not().get(0).predicate().expression().get(index).full_column_name().table_name() != null) {
            return new ColumnItem(
                    ctx.search_condition_and().get(0).search_condition_not().get(0).predicate().expression().get(index).full_column_name().table_name().database != null
                            ? ctx.search_condition_and().get(0).search_condition_not().get(0).predicate().expression().get(index).full_column_name().table_name().database.getText()
                            : null,
                    ctx.search_condition_and().get(0).search_condition_not().get(0).predicate().expression().get(index).full_column_name().table_name().schema != null
                            ? ctx.search_condition_and().get(0).search_condition_not().get(0).predicate().expression().get(index).full_column_name().table_name().schema.getText()
                            : null,
                    new TableItem(null,
                            ctx.search_condition_and().get(0).search_condition_not().get(0).predicate().expression().get(index).full_column_name().table_name().table != null
                                    ? ctx.search_condition_and().get(0).search_condition_not().get(0).predicate().expression().get(index).full_column_name().table_name().table.getText()
                                    : null),
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

    public TableItem getTable() {
        return table;
    }

    public void setTable(TableItem table) {
        this.table = table;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return "ColumnItem{" +
                "database='" + database + '\'' +
                ", schema='" + schema + '\'' +
                ", table={" + table + "}" +
                ", name='" + name + '\'' +
                '}';
    }
}

package DP.Database;

import DP.antlr4.tsql.parser.TSqlParser;

import java.util.List;

public class ColumnItem {
    String database;
    String schema;
    DatabaseTable table;
    String name;
    boolean isForeignKey;
    String referencesTableName;
    String referencesColumnName;
    DatabaseTable referencesTable;
    ColumnItem referencesColumn;
    boolean isNullable = true;

    public ColumnItem(String database, String schema, DatabaseTable table, String name, boolean isForeignKey, String referencesTableName, String referencesColumnName, DatabaseTable referencesTable, ColumnItem referencesColumn, boolean isNullable) {
        this.database = database;
        this.schema = schema;
        this.table = table;
        this.name = name;
        this.isForeignKey = isForeignKey;
        this.referencesTableName = referencesTableName;
        this.referencesColumnName = referencesColumnName;
        this.referencesTable = referencesTable;
        this.referencesColumn = referencesColumn;
        this.isNullable = isNullable;
    }

    public ColumnItem(String database, String schema, DatabaseTable table, String name, boolean isNullable) {
        this.database = database;
        this.schema = schema;
        this.table = table;
        this.name = name;
        this.isForeignKey = false;
        this.isNullable = isNullable;
    }

    public ColumnItem(String database, String schema, DatabaseTable table, String name) {
        this.database = database;
        this.schema = schema;
        this.table = table;
        this.name = name;
    }

    public static ColumnItem findOrCreate(DatabaseMetadata metadata, TSqlParser.Search_condition_notContext ctx, int index) {
        if (ctx.predicate().expression().get(index).full_column_name().table_name() != null) {
            int tableIndex = DatabaseTable.exists(metadata,
                    null,
                    ctx.predicate().expression().get(index).full_column_name().table_name().table != null
                    ? ctx.predicate().expression().get(index).full_column_name().table_name().table.getText()
                    : null);
            DatabaseTable table;
            if (tableIndex == -1) {
                table = new DatabaseTable();
                table.setTableAlias(ctx.predicate().expression().get(index).full_column_name().table_name().table != null
                                ? ctx.predicate().expression().get(index).full_column_name().table_name().table.getText()
                                : null);
            } else {
                table = metadata.getTables().get(tableIndex);
            }

            if (tableIndex == -1) {
                return new ColumnItem(ctx.predicate().expression().get(index).full_column_name().table_name().database != null
                        ? ctx.predicate().expression().get(index).full_column_name().table_name().database.getText()
                        : null,
                        ctx.predicate().expression().get(index).full_column_name().table_name().schema != null
                                ? ctx.predicate().expression().get(index).full_column_name().table_name().schema.getText()
                                : null,
                        table,
                        ctx.predicate().expression().get(index).full_column_name().column_name.getText());
            }
            return table.findColumn(ctx.predicate().expression().get(index).full_column_name().column_name.getText());
        }

        return new ColumnItem(
                null,
                null,
                null,
                ctx.predicate().expression().get(index).full_column_name().column_name.getText()
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

    public boolean isForeignKey() {
        return isForeignKey;
    }

    public void setForeignKey(boolean foreignKey) {
        isForeignKey = foreignKey;
    }

    public String getReferencesTableName() {
        return referencesTableName;
    }

    public void setReferencesTableName(String referencesTableName) {
        this.referencesTableName = referencesTableName;
    }

    public String getReferencesColumnName() {
        return referencesColumnName;
    }

    public void setReferencesColumnName(String referencesColumnName) {
        this.referencesColumnName = referencesColumnName;
    }

    public DatabaseTable getReferencesTable() {
        return referencesTable;
    }

    public void setReferencesTable(DatabaseTable referencesTable) {
        this.referencesTable = referencesTable;
    }

    public ColumnItem getReferencesColumn() {
        return referencesColumn;
    }

    public void setReferencesColumn(ColumnItem referencesColumn) {
        this.referencesColumn = referencesColumn;
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

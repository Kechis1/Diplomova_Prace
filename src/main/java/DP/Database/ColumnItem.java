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
    boolean isConstant = false;
    String value;
    int startAt;
    int stopAt;

    public ColumnItem(String database, String schema, DatabaseTable table, String name, boolean isForeignKey, String referencesTableName, String referencesColumnName, DatabaseTable referencesTable, ColumnItem referencesColumn, boolean isNullable, boolean isConstant, String value) {
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
        this.isConstant = isConstant;
        this.value = value;
    }

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

    public int getStartAt() {
        return startAt;
    }

    public void setStartAt(int startAt) {
        this.startAt = startAt;
    }

    public int getStopAt() {
        return stopAt;
    }

    public void setStopAt(int stopAt) {
        this.stopAt = stopAt;
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
        } else if (metadata.getTables().size() == 1) {
            return metadata.getTables().get(0).findColumn(ctx.predicate().expression().get(index).full_column_name().column_name.getText());
        }

        return new ColumnItem(
                null,
                null,
                null,
                ctx.predicate().expression().get(index).full_column_name().column_name.getText()
        );
    }

    public static ColumnItem findOrCreate(DatabaseMetadata metadata, TSqlParser.Select_list_elemContext ctx) {
        if (ctx.column_elem().table_name() != null) {
            int tableIndex = DatabaseTable.exists(metadata,
                    null,
                    ctx.column_elem().table_name().table != null
                            ? ctx.column_elem().table_name().table.getText()
                            : null);
            DatabaseTable table;
            if (tableIndex == -1) {
                table = new DatabaseTable();
                table.setTableAlias(ctx.column_elem().table_name().table != null
                        ? ctx.column_elem().table_name().table.getText()
                        : null);
            } else {
                table = metadata.getTables().get(tableIndex);
            }

            if (tableIndex == -1) {
                ColumnItem it = new ColumnItem(ctx.column_elem().table_name().database != null
                        ? ctx.column_elem().table_name().database.getText()
                        : null,
                        ctx.column_elem().table_name().schema != null
                                ? ctx.column_elem().table_name().schema.getText()
                                : null,
                        table,
                        ctx.column_elem().column_name.getText());
                it.setStartAt(ctx.column_elem().getStart().getStartIndex());
                it.setStopAt(ctx.column_elem().getStop().getStopIndex());
                return it;
            }
            ColumnItem it = table.findColumn(ctx.column_elem().column_name.getText());
            if (it == null) {
                it = new ColumnItem(
                        null,
                        null,
                        null,
                        ctx.column_elem().column_name.getText()
                );
            }
            it.setStartAt(ctx.column_elem().getStart().getStartIndex());
            it.setStopAt(ctx.column_elem().getStop().getStopIndex());
            return it;
        } else if (metadata.getTables().size() == 1) {
            ColumnItem it = metadata.getTables().get(0).findColumn(ctx.column_elem().column_name.getText());
            it.setStartAt(ctx.column_elem().getStart().getStartIndex());
            it.setStopAt(ctx.column_elem().getStop().getStopIndex());
            return it;
        }

        ColumnItem it = new ColumnItem(
                null,
                null,
                null,
                ctx.column_elem().column_name.getText()
        );
        it.setStartAt(ctx.column_elem().getStart().getStartIndex());
        it.setStopAt(ctx.column_elem().getStop().getStopIndex());
        return it;
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

    public static ColumnItem duplicatesExists(List<ColumnItem> columns) {
        for (int i = 0; i < columns.size() - 1; i++) {
            for (int j = i + 1; j < columns.size(); j++) {
                if (columns.get(i).equals(columns.get(j))) {
                    return columns.get(i);
                }
            }
        }
        return null;
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

    public boolean isNullable() {
        return isNullable;
    }

    public void setNullable(boolean nullable) {
        isNullable = nullable;
    }

    public boolean isForeignKey() {
        return isForeignKey;
    }

    public String getReferencesColumnName() {
        return referencesColumnName;
    }

    public boolean isConstant() {
        return isConstant;
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
                ", isForeignKey=" + isForeignKey +
                ", referencesTableName='" + referencesTableName + '\'' +
                ", referencesColumnName='" + referencesColumnName + '\'' +
                ", isNullable=" + isNullable +
                ", isConstant=" + isConstant +
                ", value='" + value + '\'' +
                '}';
    }
}

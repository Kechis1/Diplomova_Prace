package DP.Database;

import DP.antlr4.tsql.parser.TSqlParser;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public class ColumnItem {
    String database;
    String schema;
    DatabaseTable table;
    String name;
    String fullName;
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
    boolean isUsedAlias;

    public ColumnItem(String database, String schema, DatabaseTable table, String name, String fullName, boolean isForeignKey, String referencesTableName, String referencesColumnName, DatabaseTable referencesTable, ColumnItem referencesColumn, boolean isNullable, boolean isConstant, String value, boolean isUsedAlias) {
        this.database = database;
        this.schema = schema;
        this.table = table;
        this.name = name;
        this.fullName = fullName;
        this.isForeignKey = isForeignKey;
        this.referencesTableName = referencesTableName;
        this.referencesColumnName = referencesColumnName;
        this.referencesTable = referencesTable;
        this.referencesColumn = referencesColumn;
        this.isNullable = isNullable;
        this.isConstant = isConstant;
        this.value = value;
        this.isUsedAlias = isUsedAlias;
    }

    public ColumnItem(String database, String schema, DatabaseTable table, String name, String fullName, boolean isForeignKey, String referencesTableName, String referencesColumnName, DatabaseTable referencesTable, ColumnItem referencesColumn, boolean isNullable, boolean isUsedAlias) {
        this.database = database;
        this.schema = schema;
        this.table = table;
        this.name = name;
        this.fullName = fullName;
        this.isForeignKey = isForeignKey;
        this.referencesTableName = referencesTableName;
        this.referencesColumnName = referencesColumnName;
        this.referencesTable = referencesTable;
        this.referencesColumn = referencesColumn;
        this.isNullable = isNullable;
        this.isUsedAlias = isUsedAlias;
    }

    public ColumnItem(String database, String schema, DatabaseTable table, String name, String fullName, boolean isNullable, boolean isUsedAlias) {
        this.database = database;
        this.schema = schema;
        this.table = table;
        this.name = name;
        this.fullName = fullName;
        this.isForeignKey = false;
        this.isNullable = isNullable;
        this.isUsedAlias = isUsedAlias;
    }

    public ColumnItem(String database, String schema, DatabaseTable table, String name, String fullName, boolean isUsedAlias) {
        this.database = database;
        this.schema = schema;
        this.table = table;
        this.name = name;
        this.fullName = fullName;
        this.isUsedAlias = isUsedAlias;
    }

    public ColumnItem() {

    }

    public ColumnItem(ColumnItem original) {
        this.database = original.database;
        this.schema = original.schema;
        this.table = original.table;
        this.name = original.name;
        this.fullName = original.fullName;
        this.isForeignKey = original.isForeignKey;
        this.referencesTableName = original.referencesTableName;
        this.referencesColumnName = original.referencesColumnName;
        this.referencesTable = original.referencesTable;
        this.referencesColumn = original.referencesColumn;
        this.isNullable = original.isNullable;
        this.isConstant = original.isConstant;
        this.value = original.value;
        this.startAt = original.startAt;
        this.stopAt = original.stopAt;
        this.isUsedAlias = original.isUsedAlias;
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

    public void setName(String name) {
        this.name = name;
    }

    public String getFullName() {
        return fullName;
    }

    public void setFullName(String fullName) {
        this.fullName = fullName;
    }

    public boolean isUsedAlias() {
        return isUsedAlias;
    }

    public void setUsedAlias(boolean usedAlias) {
        isUsedAlias = usedAlias;
    }

    public static boolean isTablesColumnsReferencedOutsideOfJoin(JoinItem join, DatabaseTable table, Map<String, ColumnItem> columnItems) {
        for (String key: columnItems.keySet()) {
            if (columnItems.get(key).getTable().equals(table) && !(join.getStartAt() < columnItems.get(key).getStartAt() && join.getStopAt() >= columnItems.get(key).getStopAt())) {
                return true;
            }
        }
        return false;
    }

    public static ColumnItem findOrCreate(DatabaseMetadata metadata, TSqlParser.Search_condition_notContext ctx, int index) {
        return build(metadata, false, ctx.predicate().expression().get(index).full_column_name().getText(), ctx.predicate().expression().get(index).full_column_name().table_name(), ctx.predicate().expression().get(index).full_column_name().column_name, -1, -1);
    }

    public static ColumnItem findOrCreate(DatabaseMetadata metadata, TSqlParser.Select_list_elemContext ctx) {
        return build(metadata, ctx.expression_elem() != null && ctx.expression_elem().as_column_alias() != null, ctx.column_elem().getText(), ctx.column_elem().table_name(), ctx.column_elem().column_name, ctx.column_elem().getStart().getStartIndex(), ctx.column_elem().getStop().getStopIndex());
    }

    public static ColumnItem findOrCreate(DatabaseMetadata metadata, TSqlParser.Full_column_nameContext ctx) {
        return build(metadata, false, ctx.getText(), ctx.table_name(), ctx.column_name, ctx.getStart().getStartIndex(), ctx.getStop().getStopIndex());
    }

    public static ColumnItem findOrCreate(DatabaseMetadata metadata, TSqlParser.Column_elemContext ctx) {
        return build(metadata, ctx.as_column_alias() != null, ctx.getText(), ctx.table_name(), ctx.column_name, ctx.getStart().getStartIndex(), ctx.getStop().getStopIndex());
    }

    private static ColumnItem build(DatabaseMetadata metadata, boolean isUsedAlias, String fullColumn, TSqlParser.Table_nameContext tableName, TSqlParser.IdContext columnName, int startIndex, int stopIndex) {
        if (tableName != null) {
            int tableIndex = DatabaseTable.exists(metadata,
                    null,
                    tableName.table != null
                            ? tableName.table.getText()
                            : null);
            DatabaseTable table;
            if (tableIndex == -1) {
                table = new DatabaseTable();
                table.setTableAlias(tableName.table != null
                        ? tableName.table.getText()
                        : null);
                ColumnItem it = new ColumnItem(tableName.database != null
                        ? tableName.database.getText()
                        : null,
                        tableName.schema != null
                                ? tableName.schema.getText()
                                : null,
                        table,
                        columnName.getText(),
                        fullColumn,
                        isUsedAlias
                        );
                if (startIndex != -1 && stopIndex != -1) {
                    it.setStartAt(startIndex);
                    it.setStopAt(stopIndex);
                }
                return it;
            } else {
                table = metadata.getTables().get(tableIndex);
            }

            ColumnItem original = table.findColumn(columnName.getText());
            ColumnItem it;
            if (original == null) {
                it = new ColumnItem(
                        null,
                        null,
                        null,
                        columnName.getText(),
                        fullColumn,
                        isUsedAlias
                );
            } else {
                it = new ColumnItem(original);
            }
            it.setFullName(fullColumn);
            it.setUsedAlias(isUsedAlias);
            if (startIndex != -1 && stopIndex != -1) {
                it.setStartAt(startIndex);
                it.setStopAt(stopIndex);
            }
            return it;
        }

        for (DatabaseTable t: metadata.getTables()) {
            ColumnItem original = t.findColumn(columnName.getText());
            ColumnItem c = new ColumnItem(original);
            c.setUsedAlias(isUsedAlias);
            if (c.getTable() != null) {
                c.setFullName(fullColumn);
                if (startIndex != -1 && stopIndex != -1) {
                    c.setStartAt(startIndex);
                    c.setStopAt(stopIndex);
                }
                return c;
            }
        }

        ColumnItem c = new ColumnItem(
                null,
                null,
                null,
                columnName.getText(),
                fullColumn,
                isUsedAlias
        );
        if (startIndex != -1 && stopIndex != -1) {
            c.setStartAt(startIndex);
            c.setStopAt(stopIndex);
        }
        return c;
    }

    public static boolean exists(List<ColumnItem> allColumns, ColumnItem columnItem) {
        if (allColumns == null) return false;
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

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ColumnItem that = (ColumnItem) o;
        return Objects.equals(database, that.database) && Objects.equals(schema, that.schema) && Objects.equals(table, that.table) && Objects.equals(name, that.name);
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
                "\n\tdatabase='" + database + '\'' +
                "\n\t, schema='" + schema + '\'' +
                "\n\t, table=" + tableString +
                "\n\t, name='" + name + '\'' +
                "\n\t, fullName='" + fullName + '\'' +
                "\n\t, startAt='" + startAt + '\'' +
                "\n\t, stopAt='" + stopAt + '\'' +
                "\n\t, isForeignKey=" + isForeignKey +
                "\n\t, referencesTableName='" + referencesTableName + '\'' +
                "\n\t, referencesColumnName='" + referencesColumnName + '\'' +
                "\n\t, isNullable=" + isNullable +
                "\n\t, isConstant=" + isConstant +
                "\n\t, value='" + value + '\'' +
                "\n}";
    }
}

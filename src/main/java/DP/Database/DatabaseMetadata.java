package DP.Database;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.json.JSONObject;
import org.json.JSONTokener;
import org.json.JSONArray;


public class DatabaseMetadata {
    private String tableCatalog;
    private String tableSchema;
    private List<DatabaseTable> tables;

    public DatabaseMetadata(String tableCatalog, String tableSchema, List<DatabaseTable> tables) {
        this.tableCatalog = tableCatalog;
        this.tableSchema = tableSchema;
        this.tables = tables;
    }

    public List<DatabaseTable> getTables() {
        return tables;
    }

    public static DatabaseMetadata LoadFromJson(String path) {
        ClassLoader classloader = Thread.currentThread().getContextClassLoader();
        InputStream is = classloader.getResourceAsStream(path);

        if (is == null) {
            throw new NullPointerException("Cannot find resource file " + path);
        }

        JSONTokener tokener = new JSONTokener(is);
        JSONObject object = new JSONObject(tokener);
        JSONArray jsonTables = object.getJSONArray("tables");
        List<DatabaseTable> dbTables = new ArrayList<>();
        for (int i = 0; i < jsonTables.length(); i++) {
            JSONObject tableObject = jsonTables.getJSONObject(i);
            List<ColumnItem> tableColumns = new ArrayList<>();
            List<String> tablePrimaryKeys = new ArrayList<>();
            List<ForeignKey> tableForeignKeys = new ArrayList<>();

            DatabaseTable table = new DatabaseTable(null,
                    tableObject.getString("table_name").toUpperCase(),
                    tableColumns,
                    tablePrimaryKeys,
                    tableForeignKeys,
                    tableObject.getString("table_name").toUpperCase(),
                    tableObject.getInt("records_count")
            );
            if (tableObject.has("foreign_keys")) {
                JSONArray fKeys = tableObject.getJSONArray("foreign_keys");
                for (Object o : fKeys) {
                    JSONObject item = (JSONObject) o;
                    tableForeignKeys.add(new ForeignKey(item.getString("column_name").toUpperCase(),
                            tableColumns.stream().filter(x -> x.getName().equalsIgnoreCase(item.getString("column_name"))).findFirst().orElse(null),
                            item.getString("references_column").toUpperCase(),
                            null,
                            item.getString("references_table").toUpperCase(),
                            null));
                }
            }
            JSONArray columns = tableObject.getJSONArray("columns");
            for (Object o : columns) {
                JSONObject item = (JSONObject) o;
                int foundIndex = -1;
                for (int fI = 0; fI < tableForeignKeys.size(); fI++) {
                    if (tableForeignKeys.get(fI).getColumnName().equals(item.getString("name").toUpperCase())) {
                        foundIndex = fI;
                        break;
                    }
                }
                if (foundIndex == -1) {
                    tableColumns.add(new ColumnItem(object.getString("table_catalog").toUpperCase(),
                            object.getString("table_schema").toUpperCase(),
                            null,
                            item.getString("name").toUpperCase(),
                            item.getString("name").toUpperCase(),
                            item.getBoolean("is_nullable"),
                            false));
                } else {
                    tableColumns.add(new ColumnItem(object.getString("table_catalog").toUpperCase(),
                            object.getString("table_schema").toUpperCase(),
                            null,
                            item.getString("name").toUpperCase(),
                            item.getString("name").toUpperCase(),
                            true,
                            tableForeignKeys.get(foundIndex).getReferencesTableName(),
                            tableForeignKeys.get(foundIndex).getReferencesColumnName(),
                            tableForeignKeys.get(foundIndex).getReferencesTableObj(),
                            tableForeignKeys.get(foundIndex).getReferencesColumnObj(),
                            item.getBoolean("is_nullable"),
                            false));
                }
            }
            tableObject.getJSONArray("primary_keys").forEach(x -> tablePrimaryKeys.add(x.toString().toUpperCase()));
            table.setColumns(tableColumns);
            table.setForeignKeys(tableForeignKeys);
            table.setPrimaryKeys(tablePrimaryKeys);
            dbTables.add(table);
        }

        for (int i = 0; i < jsonTables.length(); i++) {
            JSONObject tableObject = jsonTables.getJSONObject(i);

            if (tableObject.has("foreign_keys")) {
                JSONArray fKeys = tableObject.getJSONArray("foreign_keys");
                for (int h = 0; h < fKeys.length(); h++) {
                    JSONObject item = (JSONObject) fKeys.get(h);
                    for (int j = 0; j < dbTables.size(); j++) {
                        if (j == i) {
                            continue;
                        }
                        if (dbTables.get(j).getTableName().equalsIgnoreCase(item.getString("references_table"))) {
                            ColumnItem foundCI = dbTables.get(j).getColumns().stream().filter(x -> x.getName().equalsIgnoreCase(item.getString("references_column"))).findAny().orElse(null);
                            dbTables.get(i).getForeignKeys().get(h).setReferencesColumnObj(foundCI);
                            dbTables.get(i).getForeignKeys().get(h).setReferencesTableObj(dbTables.get(j));
                        }
                    }
                }
            }
        }

        return new DatabaseMetadata(object.getString("table_catalog").toUpperCase(),
                object.getString("table_schema").toUpperCase(),
                dbTables
        );
    }

    private ColumnItem findColumn(ColumnItem columnItem) {
        DatabaseTable table = findTable(columnItem.getTable().getTableName(), columnItem.getTable().getTableAlias());
        if (table.getColumns() == null) return null;
        for (ColumnItem item : table.getColumns()) {
            if (item.getName().equalsIgnoreCase(columnItem.getName())) {
                return item;
            }
        }
        return null;
    }

    public DatabaseTable findTable(String tableName, String tableAlias) {
        for (DatabaseTable item : tables) {
            if ((item.getTableName() != null && item.getTableName().equals(tableName)) ||
                    (item.getTableAlias() != null && item.getTableAlias().equals(tableAlias))) {
                return item;
            }
        }
        return new DatabaseTable();
    }

    public int tableExists(String tableName, String tableAlias) {
        for (int i = 0; i < tables.size(); i++) {
            if ((tables.get(i).getTableName() != null && tables.get(i).getTableName().equals(tableName)) ||
                    (tables.get(i).getTableAlias() != null && tables.get(i).getTableAlias().equals(tableAlias))) {
                return i;
            }
        }
        return -1;
    }

    public ArrayList<String> getAllPrimaryKeys() {
        ArrayList<String> primaryKeys = new ArrayList<>();
        for (DatabaseTable table : tables) {
            primaryKeys.addAll(table.getPrimaryKeys());
        }
        return primaryKeys;
    }

    public DatabaseMetadata withTables(List<DatabaseTable> inTables) {
        List<DatabaseTable> newTables = new ArrayList<>();

        for (DatabaseTable item : tables) {
            for (DatabaseTable DatabaseTable : inTables) {
                if (DatabaseTable.getTableName() != null && DatabaseTable.getTableName().equals(item.getTableName())) {
                    item.setTableAlias(DatabaseTable.getTableAlias());
                    newTables.add(item);

                    if (newTables.size() == inTables.size()) {
                        tables = newTables;
                        return this;
                    }
                }
            }
        }

        return this;
    }

    public boolean columnsEqual(ColumnItem leftSideColumnItem, ColumnItem rightSideColumnItem) {
        if (leftSideColumnItem == null || rightSideColumnItem == null || rightSideColumnItem.getName() == null || leftSideColumnItem.getName() == null || !leftSideColumnItem.getName().equals(rightSideColumnItem.getName())) {
            return false;
        }
        return (leftSideColumnItem.getTable() == null && rightSideColumnItem.getTable() == null) || leftSideColumnItem.getTable() == null || leftSideColumnItem.getTable().getTableAlias() == null || rightSideColumnItem.getTable().getTableAlias() == null || leftSideColumnItem.getTable().getTableAlias().equals(rightSideColumnItem.getTable().getTableAlias());
    }

    public boolean columnExists(ColumnItem columnItem) {
        ArrayList<ColumnItem> allColumns = getAllColumnItems();
        return ColumnItem.exists(allColumns, columnItem);
    }

    private ArrayList<ColumnItem> getAllColumnItems() {
        ArrayList<ColumnItem> columns = new ArrayList<>();
        for (DatabaseTable table : tables) {
            columns.addAll(table.getColumns());
        }
        return columns;
    }

    public List<ConditionItem> setNullableColumns(List<ConditionItem> conditions) {
        for (ConditionItem conItem : conditions) {
            ColumnItem colItem = findColumn(conItem.getLeftSideColumnItem());
            if (colItem == null) continue;
            conItem.getLeftSideColumnItem().setNullable(colItem.isNullable());
            colItem = findColumn(conItem.getRightSideColumnItem());
            if (colItem == null) continue;
            conItem.getRightSideColumnItem().setNullable(colItem.isNullable());
        }
        return conditions;
    }

    @Override
    public String toString() {
        return "DatabaseMetadata{" +
                "tableCatalog='" + tableCatalog + '\'' +
                ", tableSchema='" + tableSchema + '\'' +
                ", tables=" + tables +
                '}';
    }
}

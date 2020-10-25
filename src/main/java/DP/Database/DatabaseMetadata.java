package DP.Database;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
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

    public String getTableCatalog() {
        return tableCatalog;
    }

    public void setTableCatalog(String tableCatalog) {
        this.tableCatalog = tableCatalog;
    }

    public String getTableSchema() {
        return tableSchema;
    }

    public void setTableSchema(String tableSchema) {
        this.tableSchema = tableSchema;
    }

    public List<DatabaseTable> getTables() {
        return tables;
    }

    public void setTables(List<DatabaseTable> tables) {
        this.tables = tables;
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

            DatabaseTable table = new DatabaseTable(tableObject.getString("table_name").toUpperCase(),
                    tableColumns,
                    tablePrimaryKeys,
                    tableForeignKeys,
                    tableObject.getString("table_name").toUpperCase()
            );

            JSONArray columns = tableObject.getJSONArray("columns");
            for (Object o : columns) {
                JSONObject item = (JSONObject) o;
                tableColumns.add(new ColumnItem(object.getString("table_catalog").toUpperCase(),
                        object.getString("table_schema").toUpperCase(),
                        null,
                        item.getString("name").toUpperCase(),
                        item.getBoolean("is_nullable")));
            }
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
        for (ColumnItem item : table.getColumns()) {
            if (item.getName().equalsIgnoreCase(columnItem.getName())) {
                return item;
            }
        }
        return null;
    }

    public DatabaseTable findTable(String tableName, String tableAlias) {
        for (DatabaseTable item : tables) {
            if (item.getTableName().equals(tableName) || item.getTableAlias().equals(tableAlias)) {
                return item;
            }
        }
        return new DatabaseTable();
    }

    public ArrayList<String> getAllPrimaryKeys() {
        ArrayList<String> primaryKeys = new ArrayList<>();
        for (DatabaseTable table : tables) {
            primaryKeys.addAll(table.getPrimaryKeys());
        }
        return primaryKeys;
    }

    public boolean existsInTablesPrimaryKeys(ArrayList<String> columns) {
        ArrayList<String> allPrimaryKeys = getAllPrimaryKeys();
        return allPrimaryKeys.containsAll(columns) && allPrimaryKeys.size() == columns.size();
    }

    public DatabaseMetadata withTables(List<DatabaseTable> inTables) {
        List<DatabaseTable> newTables = new ArrayList<>();

        for (DatabaseTable item : tables) {
            for (DatabaseTable DatabaseTable : inTables) {
                if (DatabaseTable.getTableName().equals(item.getTableName())) {
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
        if (!leftSideColumnItem.getName().equals(rightSideColumnItem.getName())) {
            return false;
        }
        return leftSideColumnItem.getTable().getTableAlias() == null || rightSideColumnItem.getTable().getTableAlias() == null || leftSideColumnItem.getTable().getTableAlias().equals(rightSideColumnItem.getTable().getTableAlias());
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
            assert colItem != null;
            conItem.getLeftSideColumnItem().setNullable(colItem.isNullable());
            colItem = findColumn(conItem.getRightSideColumnItem());
            assert colItem != null;
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

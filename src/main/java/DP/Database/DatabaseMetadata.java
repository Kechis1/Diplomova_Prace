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
            List<String> tableColumns = new ArrayList<>();
            List<String> tablePrimaryKeys = new ArrayList<>();

            tableObject.getJSONArray("column_names").forEach(x -> tableColumns.add(x.toString().toUpperCase()));
            tableObject.getJSONArray("primary_keys").forEach(x -> tablePrimaryKeys.add(x.toString().toUpperCase()));

            dbTables.add(new DatabaseTable(tableObject.getString("table_name").toUpperCase(),
                            tableColumns,
                            tablePrimaryKeys,
                    tableObject.getString("table_name").toUpperCase()
                    )
            );
        }

        return new DatabaseMetadata(object.getString("table_catalog"),
                object.getString("table_schema"),
                dbTables
        );
    }

    public DatabaseTable findTable(String tableName) {
        for (DatabaseTable item : tables) {
            if (item.getTableName().equals(tableName)) {
                return item;
            }
        }
        return null;
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

    public DatabaseMetadata withTables(List<TableItem> inTables) {
        List<DatabaseTable> newTables = new ArrayList<>();

        for (DatabaseTable item : tables) {
            for (TableItem tableItem : inTables) {
                if (tableItem.getName().equals(item.getTableName())) {
                    item.setTableAlias(tableItem.getAlias());
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
        return leftSideColumnItem.getTable().getAlias() == null || rightSideColumnItem.getTable().getAlias() == null || leftSideColumnItem.getTable().getAlias().equals(rightSideColumnItem.getTable().getAlias());
    }

    public boolean columnExists(ColumnItem columnItem) {
        ArrayList<ColumnItem> allColumns = getAllColumnItems();
        for (ColumnItem currentItem : allColumns) {
            if (!currentItem.getName().equals(columnItem.getName())) {
                continue;
            }
            if (columnItem.getTable().getAlias() == null || currentItem.getTable().getAlias().equals(columnItem.getTable().getAlias())) {
                return true;
            }
        }
        return false;
    }

    private ArrayList<ColumnItem> getAllColumnItems() {
        ArrayList<ColumnItem> columns = new ArrayList<>();
        for (DatabaseTable table : tables) {
            columns.addAll(table.getColumnItems());
        }
        return columns;
    }

    private ArrayList<String> getAllColumns() {
        ArrayList<String> columns = new ArrayList<>();
        for (DatabaseTable table : tables) {
            columns.addAll(table.getColumns());
        }
        return columns;
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

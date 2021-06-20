package DP.Database;

import java.util.Map;
import java.util.Set;

public class ForeignKey {
    Set<String> columnNames;
    Map<String, ColumnItem> columnItems;
    Set<String> referencesColumnNames;
    Map<String, ColumnItem> referencesColumnObjs;
    String referencesTableName;
    DatabaseTable referencesTableObj;

    public ForeignKey(Set<String> columnNames, Map<String, ColumnItem> columnItems, Set<String> referencesColumnNames, Map<String, ColumnItem> referencesColumnObjs, String referencesTableName, DatabaseTable referencesTableObj) {
        this.columnNames = columnNames;
        this.columnItems = columnItems;
        this.referencesColumnNames = referencesColumnNames;
        this.referencesColumnObjs = referencesColumnObjs;
        this.referencesTableName = referencesTableName;
        this.referencesTableObj = referencesTableObj;
    }

    public Set<String> getColumnNames() {
        return columnNames;
    }

    public void setColumnNames(Set<String> columnNames) {
        this.columnNames = columnNames;
    }

    public Map<String, ColumnItem> getColumnItems() {
        return columnItems;
    }

    public void setColumnItems(Map<String, ColumnItem> columnItems) {
        this.columnItems = columnItems;
    }

    public Set<String> getReferencesColumnNames() {
        return referencesColumnNames;
    }

    public void setReferencesColumnNames(Set<String> referencesColumnNames) {
        this.referencesColumnNames = referencesColumnNames;
    }

    public Map<String, ColumnItem> getReferencesColumnObjs() {
        return referencesColumnObjs;
    }

    public void setReferencesColumnObjs(Map<String, ColumnItem> referencesColumnObjs) {
        this.referencesColumnObjs = referencesColumnObjs;
    }

    public String getReferencesTableName() {
        return referencesTableName;
    }

    public void setReferencesTableName(String referencesTableName) {
        this.referencesTableName = referencesTableName;
    }

    public DatabaseTable getReferencesTableObj() {
        return referencesTableObj;
    }

    public void setReferencesTableObj(DatabaseTable referencesTableObj) {
        this.referencesTableObj = referencesTableObj;
    }

    @Override
    public String toString() {
        return "ForeignKey{" +
                "columnNames=" + columnNames +
                ", columnItems=" + columnItems +
                ", referencesColumnNames=" + referencesColumnNames +
                ", referencesColumnObjs=" + referencesColumnObjs +
                ", referencesTableName='" + referencesTableName + '\'' +
                ", referencesTableObj=" + referencesTableObj +
                '}';
    }
}

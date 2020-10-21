package DP.Database;

public class ForeignKey {
    String columnName;
    ColumnItem columnItem;
    String referencesColumnName;
    ColumnItem referencesColumnObj;
    String referencesTableName;
    DatabaseTable referencesTableObj;

    public ForeignKey(String columnName, ColumnItem columnItem, String referencesColumnName, ColumnItem referencesColumnObj, String referencesTableName, DatabaseTable referencesTableObj) {
        this.columnName = columnName;
        this.columnItem = columnItem;
        this.referencesColumnName = referencesColumnName;
        this.referencesColumnObj = referencesColumnObj;
        this.referencesTableName = referencesTableName;
        this.referencesTableObj = referencesTableObj;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public ColumnItem getColumnItem() {
        return columnItem;
    }

    public void setColumnItem(ColumnItem columnItem) {
        this.columnItem = columnItem;
    }

    public String getReferencesColumnName() {
        return referencesColumnName;
    }

    public void setReferencesColumnName(String referencesColumnName) {
        this.referencesColumnName = referencesColumnName;
    }

    public ColumnItem getReferencesColumnObj() {
        return referencesColumnObj;
    }

    public void setReferencesColumnObj(ColumnItem referencesColumnObj) {
        this.referencesColumnObj = referencesColumnObj;
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
                "columnName='" + columnName + '\'' +
                ", columnItem=" + columnItem +
                ", referencesColumnName='" + referencesColumnName + '\'' +
                ", referencesColumnObj=" + referencesColumnObj +
                ", referencesTableName='" + referencesTableName + '\'' +
                ", referencesTableObj=" + referencesTableObj +
                '}';
    }
}

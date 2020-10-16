package DP.Database;

public class ForeignKey {
    String columnName;
    DatabaseTable referencesTableObj;
    String referencesTableName;

    public ForeignKey(String columnName, DatabaseTable referencesTableObj, String referencesTableName) {
        this.columnName = columnName;
        this.referencesTableObj = referencesTableObj;
        this.referencesTableName = referencesTableName;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public DatabaseTable getReferencesTableObj() {
        return referencesTableObj;
    }

    public void setReferencesTableObj(DatabaseTable referencesTableObj) {
        this.referencesTableObj = referencesTableObj;
    }

    public String getReferencesTableName() {
        return referencesTableName;
    }

    public void setReferencesTableName(String referencesTableName) {
        this.referencesTableName = referencesTableName;
    }

    @Override
    public String toString() {
        return "ForeignKey{" +
                "columnName='" + columnName + '\'' +
                ", referencesTableObj=" + referencesTableObj +
                ", referencesTableName='" + referencesTableName + '\'' +
                '}';
    }
}

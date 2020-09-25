package DP.Database;

public class AggregateItem {

    private String fullColumnName;
    private String columnName;
    private String functionName;

    public AggregateItem(String fullColumnName, String columnName, String functionName) {
        this.fullColumnName = fullColumnName;
        this.columnName = columnName;
        this.functionName = functionName;
    }

    public String getFullColumnName() {
        return fullColumnName;
    }

    public void setFullColumnName(String fullColumnName) {
        this.fullColumnName = fullColumnName;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getFunctionName() {
        return functionName;
    }

    public void setFunctionName(String functionName) {
        this.functionName = functionName;
    }

    @Override
    public String toString() {
        return "AggregateItem{" +
                "fullColumnName='" + fullColumnName + '\'' +
                ", columnName='" + columnName + '\'' +
                ", functionName='" + functionName + '\'' +
                '}';
    }

    public String getFullFunctionName() {
        return this.functionName + "(" + this.fullColumnName + ")";
    }
}

package DP.Database;

public class AggregateItem {

    private int startAt;
    private int stopAt;
    private String fullColumnName;
    private String columnName;
    private String functionName;

    public AggregateItem(int startAt, int stopAt, String fullColumnName, String columnName, String functionName) {
        this.startAt = startAt;
        this.stopAt = stopAt;
        this.fullColumnName = fullColumnName;
        this.columnName = columnName;
        this.functionName = functionName;
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

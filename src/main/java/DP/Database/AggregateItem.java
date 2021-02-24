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

    public int getStopAt() {
        return stopAt;
    }

    public String getFullColumnName() {
        return fullColumnName;
    }

    public String getFunctionName() {
        return functionName;
    }

    public String getFullFunctionName() {
        return this.functionName + "(" + this.fullColumnName + ")";
    }

    @Override
    public String toString() {
        return "AggregateItem{" +
                "fullColumnName='" + fullColumnName + '\'' +
                ", columnName='" + columnName + '\'' +
                ", functionName='" + functionName + '\'' +
                '}';
    }
}

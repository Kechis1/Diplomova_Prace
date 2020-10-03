package DP.Database;

public enum ConditionDataType {
    STRING, NUMBER, BINARY, COLUMN;

    public boolean isNumeric;

    static {
        STRING.isNumeric = false;
        NUMBER.isNumeric = true;
        BINARY.isNumeric = true;
        COLUMN.isNumeric = false;
    }

}

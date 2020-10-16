package DP.Database;

public enum ConditionDataType {
    STRING,
    STRING_FLOAT,
    STRING_REAL,
    STRING_DECIMAL,
    STRING_BINARY,
    FLOAT,
    REAL,
    DECIMAL,
    BINARY,
    COLUMN;

    public boolean isNumeric;
    public boolean isString;

    static {
        STRING.isNumeric = false;
        STRING_FLOAT.isNumeric = false;
        STRING_REAL.isNumeric = false;
        STRING_DECIMAL.isNumeric = false;
        STRING_BINARY.isNumeric = false;
        FLOAT.isNumeric = true;
        REAL.isNumeric = true;
        DECIMAL.isNumeric = true;
        BINARY.isNumeric = true;
        COLUMN.isNumeric = false;

        STRING.isString = true;
        STRING_FLOAT.isString = true;
        STRING_REAL.isString = true;
        STRING_DECIMAL.isString = true;
        STRING_BINARY.isString = true;
        FLOAT.isString = false;
        REAL.isString = false;
        DECIMAL.isString = false;
        BINARY.isString = false;
        COLUMN.isString = false;
    }

}

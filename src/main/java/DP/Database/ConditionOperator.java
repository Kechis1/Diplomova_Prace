package DP.Database;

public enum ConditionOperator {
    BETWEEN,
    EXISTS,
    LIKE,
    SAMPLE,
    AO;

    public static ConditionOperator findOperatorFromString(String operator) {
        if (operator == null)
            return AO;
        switch (operator.toUpperCase()) {
            case "BETWEEN":
                return BETWEEN;
            case "EXISTS":
                return EXISTS;
            case "LIKE":
                return LIKE;
            default:
                return AO;
        }
    }
}

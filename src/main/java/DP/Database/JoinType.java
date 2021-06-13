package DP.Database;

public enum JoinType {
    LEFT,
    FULL_OUTER,
    OUTER,
    RIGHT,
    INNER;

    public static String print(JoinType joinType) {
        if (joinType == JoinType.FULL_OUTER) {
            return "FULL OUTER";
        }
        return joinType.toString();
    }
}

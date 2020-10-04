package DP.Database;

public final class SQLLogicalOperators {
    public static boolean like(String str, String expr) {
        expr = expr.toLowerCase();
        expr = expr.replace(".", "\\.");
        expr = expr.replace("?", ".");
        expr = expr.replace("%", ".*");
        str = str.toLowerCase();
        return str.matches(expr);
    }
}

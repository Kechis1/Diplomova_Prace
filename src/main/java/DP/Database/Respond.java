package DP.Database;

public class Respond {
    String originalQuery;
    String currentQuery;
    boolean unnecessaryStatement;

    public Respond(String originalQuery) {
        this.originalQuery = originalQuery;
    }

    public Respond(String originalQuery, String currentQuery, boolean unnecessaryStatement) {
        this.originalQuery = originalQuery;
        this.currentQuery = currentQuery;
        this.unnecessaryStatement = unnecessaryStatement;
    }

    public String getOriginalQuery() {
        return originalQuery;
    }

    public void setOriginalQuery(String originalQuery) {
        this.originalQuery = originalQuery;
    }

    public String getCurrentQuery() {
        return currentQuery;
    }

    public void setCurrentQuery(String currentQuery) {
        this.currentQuery = currentQuery;
    }

    public boolean isUnnecessaryStatement() {
        return unnecessaryStatement;
    }

    public void setUnnecessaryStatement(boolean unnecessaryStatement) {
        this.unnecessaryStatement = unnecessaryStatement;
    }
}

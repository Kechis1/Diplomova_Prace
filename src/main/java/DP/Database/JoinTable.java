package DP.Database;

public class JoinTable {
    private DatabaseTable databaseTable;
    private int startAt;
    private int stopAt;

    public JoinTable(DatabaseTable databaseTable, int startAt, int stopAt) {
        this.databaseTable = databaseTable;
        this.startAt = startAt;
        this.stopAt = stopAt;
    }

    public DatabaseTable getDatabaseTable() {
        return databaseTable;
    }

    public void setDatabaseTable(DatabaseTable databaseTable) {
        this.databaseTable = databaseTable;
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
}

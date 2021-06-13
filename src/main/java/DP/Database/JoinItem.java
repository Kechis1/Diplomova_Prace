package DP.Database;

import java.util.List;

public class JoinItem {
    private DatabaseTable databaseTable;
    private int startAt;
    private int stopAt;
    List<ConditionItem> conditions;
    JoinType type;

    public JoinItem() {
    }

    public JoinItem(DatabaseTable databaseTable, int startAt, int stopAt, List<ConditionItem> conditions, JoinType type) {
        this.databaseTable = databaseTable;
        this.startAt = startAt;
        this.stopAt = stopAt;
        this.conditions = conditions;
        this.type = type;
    }

    public JoinItem(DatabaseTable databaseTable, int startAt, int stopAt) {
        this.databaseTable = databaseTable;
        this.startAt = startAt;
        this.stopAt = stopAt;
    }

    public JoinItem(List<ConditionItem> conditions, JoinType type) {
        this.conditions = conditions;
        this.type = type;
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

    public JoinType getType() {
        return type;
    }

    public void setType(JoinType type) {
        this.type = type;
    }

    public List<ConditionItem> getConditions() {
        return conditions;
    }

    public void setConditions(List<ConditionItem> conditions) {
        this.conditions = conditions;
    }

    @Override
    public String toString() {
        return "JoinItem{" +
                "type=" + type +
                ", conditions=" + conditions +
                '}';
    }
}

package DP.Database;

import java.util.List;

public class ExistsItem {
    DatabaseTable table;
    boolean not;
    List<ConditionItem> conditions;
    int selectListStartAt;
    int selectListStopAt;
    String fullExists;

    public ExistsItem(int selectListStartAt, int selectListStopAt) {
        this.selectListStartAt = selectListStartAt;
        this.selectListStopAt = selectListStopAt;
    }

    public ExistsItem() {
    }

    public int getSelectListStartAt() {
        return selectListStartAt;
    }

    public int getSelectListStopAt() {
        return selectListStopAt;
    }

    public DatabaseTable getTable() {
        return table;
    }

    public void setTable(DatabaseTable table) {
        this.table = table;
    }

    public List<ConditionItem> getConditions() {
        return conditions;
    }

    public void setConditions(List<ConditionItem> conditions) {
        this.conditions = conditions;
    }

    public boolean isNot() {
        return not;
    }

    public void setNot(boolean not) {
        this.not = not;
    }

    public String getFullExists() {
        return fullExists;
    }

    public void setFullExists(String fullExists) {
        this.fullExists = fullExists;
    }

    public void setSelectListStartAt(int selectListStartAt) {
        this.selectListStartAt = selectListStartAt;
    }

    public void setSelectListStopAt(int selectListStopAt) {
        this.selectListStopAt = selectListStopAt;
    }

    @Override
    public String toString() {
        return "ExistsItem{" +
                "table=" + table +
                ", not=" + not +
                ", conditions=" + conditions +
                ", selectListStartAt=" + selectListStartAt +
                ", selectListStopAt=" + selectListStopAt +
                ", fullExists='" + fullExists + '\'' +
                '}';
    }
}

package DP.Database;

import java.util.List;

public class ExistItem {
    DatabaseTable table;
    boolean not;
    List<ConditionItem> conditions;
    int selectListStartAt;
    int selectListStopAt;
    int predicateStartAt;
    int predicateStopAt;

    public ExistItem(DatabaseTable table, List<ConditionItem> conditions, boolean not) {
        this.table = table;
        this.conditions = conditions;
        this.not = not;
    }

    public ExistItem(int selectListStartAt, int selectListStopAt) {
        this.selectListStartAt = selectListStartAt;
        this.selectListStopAt = selectListStopAt;
    }

    public ExistItem() {
    }

    public int getPredicateStartAt() {
        return predicateStartAt;
    }

    public void setPredicateStartAt(int predicateStartAt) {
        this.predicateStartAt = predicateStartAt;
    }

    public int getPredicateStopAt() {
        return predicateStopAt;
    }

    public void setPredicateStopAt(int predicateStopAt) {
        this.predicateStopAt = predicateStopAt;
    }

    public int getSelectListStartAt() {
        return selectListStartAt;
    }

    public void setSelectListStartAt(int selectListStartAt) {
        this.selectListStartAt = selectListStartAt;
    }

    public int getSelectListStopAt() {
        return selectListStopAt;
    }

    public void setSelectListStopAt(int selectListStopAt) {
        this.selectListStopAt = selectListStopAt;
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

    @Override
    public String toString() {
        return "ExistItem{" +
                "table=" + table +
                ", not=" + not +
                ", conditions=" + conditions +
                '}';
    }
}

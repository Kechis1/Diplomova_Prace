package DP.Database;

import java.util.List;

public class JoinItem {
    List<ConditionItem> conditions;
    JoinType type;

    public JoinItem() {
    }

    public JoinItem(List<ConditionItem> conditions, JoinType type) {
        this.conditions = conditions;
        this.type = type;
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

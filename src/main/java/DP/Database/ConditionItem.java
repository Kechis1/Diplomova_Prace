package DP.Database;

public class ConditionItem {
    private String leftSide;
    private String rightSide;
    private String operator;

    public ConditionItem(String leftSide, String operator, String rightSide) {
        this.leftSide = leftSide;
        this.operator = operator;
        this.rightSide = rightSide;
    }

    public ConditionItem() { }

    public String getLeftSide() {
        return leftSide;
    }

    public void setLeftSide(String leftSide) {
        this.leftSide = leftSide;
    }

    public String getOperator() {
        return operator;
    }

    public void setOperator(String operator) {
        this.operator = operator;
    }

    public String getRightSide() {
        return rightSide;
    }

    public void setRightSide(String rightSide) {
        this.rightSide = rightSide;
    }

    @Override
    public String toString() {
        return "ConditionItem{" +
                "leftSide='" + leftSide + '\'' +
                ", operator='" + operator + '\'' +
                ", rightSide='" + rightSide + '\'' +
                '}';
    }
}

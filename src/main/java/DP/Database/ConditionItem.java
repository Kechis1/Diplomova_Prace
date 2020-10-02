package DP.Database;

import DP.Exceptions.UnnecessaryStatementException;
import DP.antlr4.tsql.parser.TSqlParser;

public class ConditionItem {
    private ConditionDataType leftSideDataType;
    private String leftSideValue;
    private ConditionDataType rightSideDataType;
    private String rightSideValue;
    private String operator;


    public ConditionItem(ConditionDataType leftSideDataType, String leftSideValue, ConditionDataType rightSideDataType, String rightSideValue, String operator) {
        this.leftSideDataType = leftSideDataType;
        this.leftSideValue = leftSideValue;
        this.rightSideDataType = rightSideDataType;
        this.rightSideValue = rightSideValue;
        this.operator = operator;
    }

    public ConditionItem(String leftSideValue, String rightSideValue, String operator) {
        this.leftSideValue = leftSideValue;
        this.rightSideValue = rightSideValue;
        this.operator = operator;
    }

    public ConditionDataType getLeftSideDataType() {
        return leftSideDataType;
    }

    public void setLeftSideDataType(ConditionDataType leftSideDataType) {
        this.leftSideDataType = leftSideDataType;
    }

    public String getLeftSideValue() {
        return leftSideValue;
    }

    public void setLeftSideValue(String leftSideValue) {
        this.leftSideValue = leftSideValue;
    }

    public ConditionDataType getRightSideDataType() {
        return rightSideDataType;
    }

    public void setRightSideDataType(ConditionDataType rightSideDataType) {
        this.rightSideDataType = rightSideDataType;
    }

    public String getRightSideValue() {
        return rightSideValue;
    }

    public void setRightSideValue(String rightSideValue) {
        this.rightSideValue = rightSideValue;
    }

    public String getOperator() {
        return operator;
    }

    public void setOperator(String operator) {
        this.operator = operator;
    }

    public boolean compareStringAgainstString() {
        if ((getOperator().equals("=") && getLeftSideValue().equalsIgnoreCase(getRightSideValue())) ||
                (getOperator().equals("<>") && !getLeftSideValue().equalsIgnoreCase(getRightSideValue())) ||
                (getOperator().equals(">=") && getLeftSideValue().compareToIgnoreCase(getRightSideValue()) >= 0) ||
                (getOperator().equals(">") && getLeftSideValue().compareToIgnoreCase(getRightSideValue()) > 0) ||
                (getOperator().equals("<=") && getLeftSideValue().compareToIgnoreCase(getRightSideValue()) <= 0) ||
                (getOperator().equals("<") && getLeftSideValue().compareToIgnoreCase(getRightSideValue()) < 0)) {
            System.out.println(UnnecessaryStatementException.messageUnnecessaryStatement + " CONDITION");
            return false;
        }
        return true;
    }

    public boolean compareNumberAgainstNumber() {
        double leftSideValue = Double.parseDouble(getLeftSideValue());
        double rightSideValue = Double.parseDouble(getRightSideValue());
        if ((getOperator().equals("=") && leftSideValue == rightSideValue) ||
                (getOperator().equals("<>") && leftSideValue != rightSideValue) ||
                (getOperator().equals(">=") && leftSideValue >= rightSideValue) ||
                (getOperator().equals(">") && leftSideValue > rightSideValue) ||
                (getOperator().equals("<=") && leftSideValue <= rightSideValue) ||
                (getOperator().equals("<") && leftSideValue < rightSideValue)) {
            System.out.println(UnnecessaryStatementException.messageUnnecessaryStatement + " CONDITION");
            return false;
        }
        return true;
    }

    public boolean compareStringAgainstNumber() {
        double leftSideValue = Double.parseDouble(getLeftSideValue());
        double rightSideValue = Double.parseDouble(getRightSideValue());
        if ((getOperator().equals("=") && leftSideValue == rightSideValue) ||
                (getOperator().equals("<>") && leftSideValue != rightSideValue) ||
                (getOperator().equals(">=") && leftSideValue >= rightSideValue) ||
                (getOperator().equals(">") && leftSideValue > rightSideValue) ||
                (getOperator().equals("<=") && leftSideValue <= rightSideValue) ||
                (getOperator().equals("<") && leftSideValue < rightSideValue)) {
            System.out.println(UnnecessaryStatementException.messageUnnecessaryStatement + " CONDITION");
            return false;
        }
        return true;
    }

    public boolean compareNumberAgainstString() {
        double leftSideValue = Double.parseDouble(getLeftSideValue());
        double rightSideValue = Double.parseDouble(getRightSideValue());
        if ((getOperator().equals("=") && leftSideValue == rightSideValue) ||
                (getOperator().equals("<>") && leftSideValue != rightSideValue) ||
                (getOperator().equals(">=") && leftSideValue >= rightSideValue) ||
                (getOperator().equals(">") && leftSideValue > rightSideValue) ||
                (getOperator().equals("<=") && leftSideValue <= rightSideValue) ||
                (getOperator().equals("<") && leftSideValue < rightSideValue)) {
            System.out.println(UnnecessaryStatementException.messageUnnecessaryStatement + " CONDITION");
            return false;
        }
        return true;
    }

    public static String findSideValue(TSqlParser.ExpressionContext context) {
        if (context.full_column_name() != null) {
            return context.full_column_name().getText();
        }
        return context.primitive_expression().getText().replaceAll("'", "");
    }

    public static ConditionDataType findDataType(TSqlParser.ExpressionContext context) {
        if (context.full_column_name() != null) {
            return ConditionDataType.COLUMN;
        }
        if (context.primitive_expression().constant().STRING() != null) {
            return ConditionDataType.STRING;
        }
        return ConditionDataType.NUMBER;
    }

    @Override
    public String toString() {
        return "ConditionItem{" +
                "leftSideDataType='" + leftSideDataType + '\'' +
                ", leftSideValue='" + leftSideValue + '\'' +
                ", rightSideDataType='" + rightSideDataType + '\'' +
                ", rightSideValue='" + rightSideValue + '\'' +
                ", operator='" + operator + '\'' +
                '}';
    }
}

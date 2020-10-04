package DP.Database;

import DP.Exceptions.UnnecessaryStatementException;
import DP.antlr4.tsql.parser.TSqlParser;

public class ConditionItem {
    private ConditionDataType leftSideDataType;
    private String leftSideValue;
    private ConditionDataType rightSideDataType;
    private String rightSideValue;
    private String operator;
    private double leftSideNumberValue;
    private double rightSideNumberValue;


    public ConditionItem(ConditionDataType leftSideDataType, String leftSideValue, ConditionDataType rightSideDataType, String rightSideValue, String operator) {
        this.leftSideDataType = leftSideDataType;
        this.leftSideValue = leftSideValue;
        this.rightSideDataType = rightSideDataType;
        this.rightSideValue = rightSideValue;
        this.operator = operator;
        initNumberValues();
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

    public double getLeftSideNumberValue() {
        return leftSideNumberValue;
    }

    public void setLeftSideNumberValue(double leftSideNumberValue) {
        this.leftSideNumberValue = leftSideNumberValue;
    }

    public double getRightSideNumberValue() {
        return rightSideNumberValue;
    }

    public void setRightSideNumberValue(double rightSideNumberValue) {
        this.rightSideNumberValue = rightSideNumberValue;
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
        if ((getOperator().equals("=") && getLeftSideNumberValue() == getRightSideNumberValue()) ||
                (getOperator().equals("<>") && getLeftSideNumberValue() != getRightSideNumberValue()) ||
                (getOperator().equals(">=") && getLeftSideNumberValue() >= getRightSideNumberValue()) ||
                (getOperator().equals(">") && getLeftSideNumberValue() > getRightSideNumberValue()) ||
                (getOperator().equals("<=") && getLeftSideNumberValue() <= getRightSideNumberValue()) ||
                (getOperator().equals("<") && getLeftSideNumberValue() < getRightSideNumberValue())) {
            System.out.println(UnnecessaryStatementException.messageUnnecessaryStatement + " CONDITION");
            return false;
        }
        return true;
    }

    public boolean compareStringAgainstNumber() {
        if ((getOperator().equals("=") && getLeftSideNumberValue() == getRightSideNumberValue()) ||
                (getOperator().equals("<>") && getLeftSideNumberValue() != getRightSideNumberValue()) ||
                (getOperator().equals(">=") && getLeftSideNumberValue() >= getRightSideNumberValue()) ||
                (getOperator().equals(">") && getLeftSideNumberValue() > getRightSideNumberValue()) ||
                (getOperator().equals("<=") && getLeftSideNumberValue() <= getRightSideNumberValue()) ||
                (getOperator().equals("<") && getLeftSideNumberValue() < getRightSideNumberValue())) {
            System.out.println(UnnecessaryStatementException.messageUnnecessaryStatement + " CONDITION");
            return false;
        }
        return true;
    }

    public boolean compareNumberAgainstString() {
        if ((getOperator().equals("=") && getLeftSideNumberValue() == getRightSideNumberValue()) ||
                (getOperator().equals("<>") && getLeftSideNumberValue() != getRightSideNumberValue()) ||
                (getOperator().equals(">=") && getLeftSideNumberValue() >= getRightSideNumberValue()) ||
                (getOperator().equals(">") && getLeftSideNumberValue() > getRightSideNumberValue()) ||
                (getOperator().equals("<=") && getLeftSideNumberValue() <= getRightSideNumberValue()) ||
                (getOperator().equals("<") && getLeftSideNumberValue() < getRightSideNumberValue())) {
            System.out.println(UnnecessaryStatementException.messageUnnecessaryStatement + " CONDITION");
            return false;
        }
        return true;
    }

    private void initNumberValues() {
        try {
            if (getLeftSideDataType() != ConditionDataType.COLUMN) {
                leftSideNumberValue = initSideValue(getLeftSideValue(), getLeftSideDataType());
            }
            if (getRightSideDataType() != ConditionDataType.COLUMN) {
                rightSideNumberValue = initSideValue(getRightSideValue(), getRightSideDataType());
            }
        } catch (NumberFormatException ignored) {

        }
    }

    private double initSideValue(String sideValue, ConditionDataType sideDataType) {
        if (sideDataType == ConditionDataType.BINARY) {
            return Integer.valueOf(sideValue, 16);
        }
        return Double.parseDouble(sideValue);
    }

    public static String findSideValue(TSqlParser.ExpressionContext context) {
        if (context.full_column_name() != null) {
            return context.full_column_name().getText();
        }
        return context.primitive_expression().getText().replaceAll("'", "").replaceFirst("0x|0X", "");
    }

    public static ConditionDataType findDataType(TSqlParser.ExpressionContext context) {
        if (context.full_column_name() != null) {
            return ConditionDataType.COLUMN;
        }
        if (context.primitive_expression().constant().STRING() != null) {
            return ConditionDataType.STRING;
        }
        if (context.primitive_expression().constant().BINARY() != null) {
            return ConditionDataType.BINARY;
        }
        return ConditionDataType.NUMBER;
    }

    @Override
    public String toString() {
        return "ConditionItem{" +
                "leftSideDataType=" + leftSideDataType +
                ", leftSideValue='" + leftSideValue + '\'' +
                ", rightSideDataType=" + rightSideDataType +
                ", rightSideValue='" + rightSideValue + '\'' +
                ", operator='" + operator + '\'' +
                ", leftSideNumberValue=" + leftSideNumberValue +
                ", rightSideNumberValue=" + rightSideNumberValue +
                '}';
    }
}

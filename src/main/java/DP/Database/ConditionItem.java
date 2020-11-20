package DP.Database;

import DP.Exceptions.UnnecessaryStatementException;
import DP.antlr4.tsql.parser.TSqlParser;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class ConditionItem {
    private ColumnItem leftSideColumnItem;
    private ColumnItem rightSideColumnItem;

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

    public static List<ConditionItem> filterByMetadata(DatabaseMetadata metadata, List<ConditionItem> conditions) {
        List<ConditionItem> items = new ArrayList<>();

        for (ConditionItem condition : conditions) {
            if (metadata.columnExists(condition.getLeftSideColumnItem()) && metadata.columnExists(condition.getRightSideColumnItem())) {
                items.add(condition);
            }
        }

        return items;
    }

    public static boolean duplicatesExists(DatabaseMetadata metadata, HashMap<Integer, List<ConditionItem>> conditions) {
        for (int x = 0; x < conditions.size(); x++) {
            for (int i = 0; i < conditions.get(x).size() - 1; i++) {
                for (int j = i + 1; j < conditions.get(x).size(); j++) {
                    if (!conditions.get(x).get(i).compareToCondition(metadata, conditions.get(x).get(j))) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    public static boolean duplicatesExists(DatabaseMetadata metadata, List<ConditionItem> conditions) {
        for (int i = 0; i < conditions.size() - 1; i++) {
            for (int j = i + 1; j < conditions.size(); j++) {
                if (!conditions.get(i).compareToCondition(metadata, conditions.get(j))) {
                    return true;
                }
            }
        }
        return false;
    }

    public static boolean isConditionColumnNullable(List<ConditionItem> conditions, DatabaseTable table, boolean checkBothSides) {
        if (checkBothSides) {
            for (ConditionItem currItem: conditions) {
                if ((currItem.getLeftSideDataType() == ConditionDataType.COLUMN && currItem.getLeftSideColumnItem().isNullable) ||
                        (currItem.getRightSideDataType() == ConditionDataType.COLUMN && currItem.getRightSideColumnItem().isNullable)) {
                    return true;
                }
            }
            return false;
        }
        for (ConditionItem currItem: conditions) {
            if ((currItem.getLeftSideDataType() == ConditionDataType.COLUMN && currItem.getLeftSideColumnItem().isNullable() && ColumnItem.exists(table.getColumns(), currItem.getLeftSideColumnItem())) ||
                    (currItem.getRightSideDataType() == ConditionDataType.COLUMN && currItem.getRightSideColumnItem().isNullable() && ColumnItem.exists(table.getColumns(), currItem.getRightSideColumnItem()))) {
                return true;
            }
        }
        return false;
    }

    public static boolean isComparingForeignKey(DatabaseTable from, DatabaseTable to, ConditionItem item) {
        if (item.getOperator().equals("=") && item.getLeftSideDataType() == ConditionDataType.COLUMN &&
                item.getRightSideDataType() == ConditionDataType.COLUMN && item.getLeftSideColumnItem() != null &&
                item.getRightSideColumnItem() != null) {
            if (item.getLeftSideColumnItem().getTable().equals(from) && item.getRightSideColumnItem().getTable().equals(to)) {
                return item.getLeftSideColumnItem().isForeignKey() && !item.getLeftSideColumnItem().isNullable() && item.getLeftSideColumnItem().getReferencesColumnName().equals(item.getRightSideColumnItem().getName());
            }
            if (item.getLeftSideColumnItem().getTable().equals(to) && item.getRightSideColumnItem().getTable().equals(from)) {
                return item.getRightSideColumnItem().isForeignKey() && !item.getRightSideColumnItem().isNullable() && item.getRightSideColumnItem().getReferencesColumnName().equals(item.getLeftSideColumnItem().getName());
            }
        }
        return false;
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
            return false;
        }
        return true;
    }

    public boolean compareColumnAgainstColumn(DatabaseMetadata metadata) {
        if (metadata.columnsEqual(getLeftSideColumnItem(), getRightSideColumnItem())) {
            return false;
        }
        return true;
    }

    public boolean compareToCondition(DatabaseMetadata metadata, ConditionItem conditionItem) {
        if (getLeftSideDataType() == ConditionDataType.COLUMN && getLeftSideDataType() == ConditionDataType.COLUMN &&
                conditionItem.getLeftSideDataType() == ConditionDataType.COLUMN && conditionItem.getLeftSideDataType() == ConditionDataType.COLUMN) {
            if (metadata.columnsEqual(getLeftSideColumnItem(), conditionItem.getLeftSideColumnItem()) &&
                    metadata.columnsEqual(getRightSideColumnItem(), conditionItem.getRightSideColumnItem())) {
                System.out.println(UnnecessaryStatementException.messageUnnecessaryStatement + " DUPLICATE CONDITION");
                return false;
            }
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
            String value = context.primitive_expression().constant().getText().replaceAll("'", "");
            if (value.toLowerCase().startsWith("0x") && value.toLowerCase().replaceFirst("0x", "").matches("-?[0-9a-fA-F]+")) {
                return ConditionDataType.STRING_BINARY;
            } else if (value.toLowerCase().matches("^(-?|\\+?)\\d+(\\.\\d+)?e(-?|\\+?)\\d+$")) {
                return ConditionDataType.STRING_REAL;
            }
            try {
                Integer.parseInt(value);
                return ConditionDataType.STRING_DECIMAL;
            } catch (NumberFormatException ignored) {
            }
            try {
                Float.parseFloat(value);
                return ConditionDataType.STRING_FLOAT;
            } catch (NumberFormatException ignored) {
            }

            return ConditionDataType.STRING;
        }
        if (context.primitive_expression().constant().BINARY() != null) {
            return ConditionDataType.BINARY;
        }
        if (context.primitive_expression().constant().REAL() != null) {
            return ConditionDataType.REAL;
        }
        if (context.primitive_expression().constant().FLOAT() != null) {
            return ConditionDataType.FLOAT;
        }
        return ConditionDataType.DECIMAL;
    }

    public ColumnItem getLeftSideColumnItem() {
        return leftSideColumnItem;
    }

    public void setLeftSideColumnItem(ColumnItem leftSideColumnItem) {
        this.leftSideColumnItem = leftSideColumnItem;
    }

    public ColumnItem getRightSideColumnItem() {
        return rightSideColumnItem;
    }

    public void setRightSideColumnItem(ColumnItem rightSideColumnItem) {
        this.rightSideColumnItem = rightSideColumnItem;
    }

    @Override
    public String toString() {
        return "ConditionItem {" +
                "\n\tleftSideColumnItem=" + leftSideColumnItem +
                "\n\trightSideColumnItem=" + rightSideColumnItem +
                "\n\tleftSideDataType=" + leftSideDataType +
                "\n\tleftSideValue='" + leftSideValue + '\'' +
                "\n\trightSideDataType=" + rightSideDataType +
                "\n\trightSideValue='" + rightSideValue + '\'' +
                "\n\toperator='" + operator + '\'' +
                "\n\tleftSideNumberValue=" + leftSideNumberValue +
                "\n\trightSideNumberValue=" + rightSideNumberValue +
                "\n}";
    }
}

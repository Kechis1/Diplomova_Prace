package DP.Database;

import DP.Exceptions.UnnecessaryStatementException;
import DP.antlr4.tsql.parser.TSqlParser;
import DP.antlr4.tsql.parser.TSqlParserBaseListener;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    public static List<String> findTablesList(ParseTree select) {
        final List<String> allTables = new ArrayList<>();
        ParseTreeWalker.DEFAULT.walk(new TSqlParserBaseListener() {
            @Override
            public void exitTable_source_item(@NotNull TSqlParser.Table_source_itemContext ctx) {
                allTables.add(ctx.table_name_with_hint().table_name().table.getText());
            }
        }, select);
        return allTables;
    }

    public static Map<String, List<String>> findJoinTablesList(ParseTree select) {
        final List<String> outerJoinTables = new ArrayList<>();
        final List<String> innerJoinTables = new ArrayList<>();
        ParseTreeWalker.DEFAULT.walk(new TSqlParserBaseListener() {
            @Override
            public void enterTable_source_item_joined(TSqlParser.Table_source_item_joinedContext ctx) {
                for (int i = 0; i < ctx.join_part().size(); i++) {
                    if (ctx.join_part().get(i).OUTER() != null || ctx.join_part().get(i).LEFT() != null || ctx.join_part().get(i).RIGHT() != null) {
                        outerJoinTables.add(ctx.join_part().get(i).table_source().table_source_item_joined().table_source_item().table_name_with_hint().table_name().table.getText());
                    } else {
                        innerJoinTables.add(ctx.join_part().get(i).table_source().table_source_item_joined().table_source_item().table_name_with_hint().table_name().table.getText());
                    }
                }
            }
        }, select);
        Map<String,List<String>> map = new HashMap();
        map.put("outerJoin", outerJoinTables);
        map.put("innerJoin", innerJoinTables);
        return map;
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

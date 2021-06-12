package DP.Database;

import DP.Transformations.Action;
import DP.Transformations.JoinConditionTransformation;
import DP.Transformations.Query;
import DP.Transformations.Transformation;
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
    private ConditionOperator operatorType;
    private String fullCondition;
    private String leftSideFullCondition;
    private String rightSideFullCondition;
    private double leftSideNumberValue;
    private double rightSideNumberValue;
    private int startAt;
    private int stopAt;
    private int leftLogicalOperatorStartAt = -1;
    private int leftLogicalOperatorStopAt = -1;
    private int rightLogicalOperatorStartAt = -1;
    private int rightLogicalOperatorStopAt = -1;
    private String leftLogicalOperator;
    private String rightLogicalOperator;
    private ConditionItem betweenLeftCondition;
    private ConditionItem betweenRightCondition;
    private ExistItem existItem;

    public ConditionItem(int startAt, int stopAt, ConditionDataType leftSideDataType, String leftSideValue, ConditionDataType rightSideDataType, String rightSideValue, String operator, ConditionOperator operatorType, String fullCondition, String leftSideFullCondition, String rightSideFullCondition) {
        this.startAt = startAt;
        this.stopAt = stopAt;
        this.operatorType = operatorType;
        this.leftSideDataType = leftSideDataType;
        this.leftSideValue = leftSideValue;
        this.rightSideDataType = rightSideDataType;
        this.rightSideValue = rightSideValue;
        this.operator = operator;
        this.fullCondition = fullCondition;
        this.leftSideFullCondition = leftSideFullCondition;
        this.rightSideFullCondition = rightSideFullCondition;
        initNumberValues();
    }

    public ConditionItem(ConditionDataType leftSideDataType, String leftSideValue, ConditionDataType rightSideDataType, String rightSideValue, String operator, String fullCondition, String leftSideFullCondition, String rightSideFullCondition) {
        this.leftSideDataType = leftSideDataType;
        this.leftSideValue = leftSideValue;
        this.rightSideDataType = rightSideDataType;
        this.rightSideValue = rightSideValue;
        this.operator = operator;
        this.fullCondition = fullCondition;
        this.leftSideFullCondition = leftSideFullCondition;
        this.rightSideFullCondition = rightSideFullCondition;
        initNumberValues();
    }

    public ConditionItem() {

    }

    public ExistItem getExistItem() {
        return existItem;
    }

    public void setExistItem(ExistItem existItem) {
        this.existItem = existItem;
    }

    public ConditionOperator getOperatorType() {
        return operatorType;
    }

    public void setOperatorType(ConditionOperator operatorType) {
        this.operatorType = operatorType;
    }

    public ConditionItem getBetweenLeftCondition() {
        return betweenLeftCondition;
    }

    public void setBetweenLeftCondition(ConditionItem betweenLeftCondition) {
        this.betweenLeftCondition = betweenLeftCondition;
    }

    public ConditionItem getBetweenRightCondition() {
        return betweenRightCondition;
    }

    public void setBetweenRightCondition(ConditionItem betweenRightCondition) {
        this.betweenRightCondition = betweenRightCondition;
    }

    public static boolean duplicatesExists(Query query, DatabaseMetadata metadata, HashMap<Integer, List<ConditionItem>> conditions) {
        for (int x = 0; x < conditions.size(); x++) {
            for (int i = 0; i < conditions.get(x).size() - 1; i++) {
                for (int j = i + 1; j < conditions.get(x).size(); j++) {
                    if (conditions.get(x).get(i).compareToCondition(metadata, conditions.get(x).get(j))) {
                        String newQueryString;

                        if (conditions.get(x).get(j).getLeftLogicalOperatorStartAt() != -1) {
                            newQueryString = (query.getCurrentQuery().substring(0, conditions.get(x).get(j).getLeftLogicalOperatorStartAt()) + query.getCurrentQuery().substring(conditions.get(x).get(j).getStopAt())).trim();
                        } else if (conditions.get(x).get(j).getRightLogicalOperatorStartAt() != -1) {
                            newQueryString = (query.getCurrentQuery().substring(0, conditions.get(x).get(j).getStartAt()) + query.getCurrentQuery().substring(conditions.get(x).get(j).getRightLogicalOperatorStopAt() + 1)).trim();
                        } else {
                            newQueryString = (query.getCurrentQuery().substring(0, conditions.get(x).get(j).getStartAt()) + query.getCurrentQuery().substring(conditions.get(x).get(j).getStopAt())).trim();
                        }

                        query.addTransformation(new Transformation(query.getCurrentQuery(),
                                newQueryString,
                                UnnecessaryStatementException.messageUnnecessaryStatement + " DUPLICATE CONDITION",
                                Action.JoinConditionTransformation,
                                true,
                                null
                        ));
                        return true;
                    }
                }
            }
        }
        return false;
    }

    public static boolean duplicatesExists(Query query, DatabaseMetadata metadata, List<ConditionItem> conditions) {
        int whereStartsAt = query.getCurrentQuery().indexOf("WHERE");
        for (int i = 0; i < conditions.size() - 1; i++) {
            for (int j = i + 1; j < conditions.size(); j++) {
                if (conditions.get(i).compareToCondition(metadata, conditions.get(j))) {
                    String newQueryString;

                    if (whereStartsAt != -1 && conditions.get(j).getStartAt() > whereStartsAt) {
                        int whereConditionSize = 0;
                        for (ConditionItem condition : conditions) {
                            if (condition.getStartAt() > whereStartsAt) {
                                whereConditionSize++;
                            }
                        }
                        newQueryString = whereConditionSize == 1 ? (query.getCurrentQuery().substring(0, whereStartsAt) + query.getCurrentQuery().substring(conditions.get(j).getStopAt()).trim()).trim() :
                                (query.getCurrentQuery().substring(0, conditions.get(j).getStartAt()) + query.getCurrentQuery().substring(conditions.get(j).getStopAt())).trim();
                    } else if (conditions.get(j).getLeftLogicalOperatorStartAt() != -1) {
                        newQueryString = (query.getCurrentQuery().substring(0, conditions.get(j).getLeftLogicalOperatorStartAt()) + query.getCurrentQuery().substring(conditions.get(j).getStopAt())).trim();
                    } else if (conditions.get(j).getRightLogicalOperatorStartAt() != -1) {
                        newQueryString = (query.getCurrentQuery().substring(0, conditions.get(j).getStartAt()) + query.getCurrentQuery().substring(conditions.get(j).getRightLogicalOperatorStopAt() + 1)).trim();
                    } else {
                        newQueryString = (query.getCurrentQuery().substring(0, conditions.get(j).getStartAt()) + query.getCurrentQuery().substring(conditions.get(j).getStopAt())).trim();
                    }
                    query.addTransformation(new Transformation(query.getCurrentQuery(),
                            newQueryString,
                            UnnecessaryStatementException.messageUnnecessaryStatement + " DUPLICATE CONDITION",
                            Action.JoinConditionTransformation,
                            true,
                            null
                    ));
                    return true;
                }
            }
        }
        return false;
    }

    public static boolean isConditionColumnNullable(List<ConditionItem> conditions, DatabaseTable table, boolean checkBothSides) {
        if (checkBothSides) {
            for (ConditionItem currItem : conditions) {
                if ((currItem.getLeftSideDataType() == ConditionDataType.COLUMN && currItem.getLeftSideColumnItem().isNullable) ||
                        (currItem.getRightSideDataType() == ConditionDataType.COLUMN && currItem.getRightSideColumnItem().isNullable)) {
                    return true;
                }
            }
            return false;
        }
        for (ConditionItem currItem : conditions) {
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

    public static List<ConditionItem> removeMultipleAttributeConditions(List<ConditionItem> conditions) {
        List<ConditionItem> newConditions = new ArrayList<>(conditions);
        boolean found;
        for (int i = 0; i < conditions.size() - 1; i++) {
            found = false;
            for (int j = i + 1; j < conditions.size(); j++) {
                if (conditions.get(i).getLeftSideDataType() != ConditionDataType.COLUMN && conditions.get(i).getRightSideDataType() != ConditionDataType.COLUMN) {
                    break;
                }
                if (conditions.get(j).getLeftSideDataType() != ConditionDataType.COLUMN && conditions.get(j).getRightSideDataType() != ConditionDataType.COLUMN) {
                    continue;
                }

                if ((conditions.get(i).getLeftSideDataType() == ConditionDataType.COLUMN &&
                        ((conditions.get(j).getLeftSideDataType() == ConditionDataType.COLUMN && conditions.get(i).getLeftSideColumnItem().equals(conditions.get(j).getLeftSideColumnItem())) ||
                                (conditions.get(j).getRightSideDataType() == ConditionDataType.COLUMN && conditions.get(i).getLeftSideColumnItem().equals(conditions.get(j).getRightSideColumnItem())))) ||
                        (conditions.get(i).getRightSideDataType() == ConditionDataType.COLUMN &&
                                ((conditions.get(j).getLeftSideDataType() == ConditionDataType.COLUMN && conditions.get(i).getRightSideColumnItem().equals(conditions.get(j).getLeftSideColumnItem())) ||
                                        (conditions.get(j).getRightSideDataType() == ConditionDataType.COLUMN && conditions.get(i).getRightSideColumnItem().equals(conditions.get(j).getRightSideColumnItem()))))) {
                    newConditions.remove(j);
                    found = true;
                }
            }
            if (found) {
                newConditions.remove(i);
            }
        }
        return newConditions;
    }

    public static List<ConditionItem> filterByOperator(List<ConditionItem> conditions, ConditionOperator operator) {
        List<ConditionItem> items = new ArrayList<>();

        for (ConditionItem item : conditions) {
            if (item.getOperatorType().equals(operator)) {
                items.add(item);
            }
        }
        return items;
    }

    public String getLeftLogicalOperator() {
        return leftLogicalOperator;
    }

    public void setLeftLogicalOperator(String leftLogicalOperator) {
        this.leftLogicalOperator = leftLogicalOperator;
    }

    public String getRightLogicalOperator() {
        return rightLogicalOperator;
    }

    public void setRightLogicalOperator(String rightLogicalOperator) {
        this.rightLogicalOperator = rightLogicalOperator;
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

    public ConditionDataType getLeftSideDataType() {
        return leftSideDataType;
    }

    public String getLeftSideValue() {
        return leftSideValue;
    }

    public ConditionDataType getRightSideDataType() {
        return rightSideDataType;
    }

    public String getRightSideValue() {
        return rightSideValue;
    }

    public String getOperator() {
        return operator;
    }

    public double getLeftSideNumberValue() {
        return leftSideNumberValue;
    }

    public double getRightSideNumberValue() {
        return rightSideNumberValue;
    }

    public boolean compareStringAgainstString() {
        return (!getOperator().equals("=") || !getLeftSideValue().equalsIgnoreCase(getRightSideValue())) &&
                (!getOperator().equals("<>") || getLeftSideValue().equalsIgnoreCase(getRightSideValue())) &&
                (!getOperator().equals(">=") || getLeftSideValue().compareToIgnoreCase(getRightSideValue()) < 0) &&
                (!getOperator().equals(">") || getLeftSideValue().compareToIgnoreCase(getRightSideValue()) <= 0) &&
                (!getOperator().equals("<=") || getLeftSideValue().compareToIgnoreCase(getRightSideValue()) > 0) &&
                (!getOperator().equals("<") || getLeftSideValue().compareToIgnoreCase(getRightSideValue()) >= 0);
    }

    public boolean compareNumberAgainstNumber() {
        return (!getOperator().equals("=") || getLeftSideNumberValue() != getRightSideNumberValue()) &&
                (!getOperator().equals("<>") || getLeftSideNumberValue() == getRightSideNumberValue()) &&
                (!getOperator().equals(">=") || !(getLeftSideNumberValue() >= getRightSideNumberValue())) &&
                (!getOperator().equals(">") || !(getLeftSideNumberValue() > getRightSideNumberValue())) &&
                (!getOperator().equals("<=") || !(getLeftSideNumberValue() <= getRightSideNumberValue())) &&
                (!getOperator().equals("<") || !(getLeftSideNumberValue() < getRightSideNumberValue()));
    }

    public boolean compareColumnAgainstColumn(DatabaseMetadata metadata) {
        return !metadata.columnsEqual(getLeftSideColumnItem(), getRightSideColumnItem());
    }

    public boolean compareToCondition(DatabaseMetadata metadata, ConditionItem conditionItem) {
        if (getLeftSideDataType() == ConditionDataType.COLUMN && getLeftSideDataType() == ConditionDataType.COLUMN &&
                conditionItem.getLeftSideDataType() == ConditionDataType.COLUMN && conditionItem.getLeftSideDataType() == ConditionDataType.COLUMN) {
            return metadata.columnsEqual(getLeftSideColumnItem(), conditionItem.getLeftSideColumnItem()) &&
                    metadata.columnsEqual(getRightSideColumnItem(), conditionItem.getRightSideColumnItem());
        }
        return false;
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

    public int getLeftLogicalOperatorStartAt() {
        return leftLogicalOperatorStartAt;
    }

    public void setLeftLogicalOperatorStartAt(int leftLogicalOperatorStartAt) {
        this.leftLogicalOperatorStartAt = leftLogicalOperatorStartAt;
    }

    public void setLeftLogicalOperatorStopAt(int leftLogicalOperatorStopAt) {
        this.leftLogicalOperatorStopAt = leftLogicalOperatorStopAt;
    }

    public int getRightLogicalOperatorStartAt() {
        return rightLogicalOperatorStartAt;
    }

    public void setRightLogicalOperatorStartAt(int rightLogicalOperatorStartAt) {
        this.rightLogicalOperatorStartAt = rightLogicalOperatorStartAt;
    }

    public int getRightLogicalOperatorStopAt() {
        return rightLogicalOperatorStopAt;
    }

    public void setRightLogicalOperatorStopAt(int rightLogicalOperatorStopAt) {
        this.rightLogicalOperatorStopAt = rightLogicalOperatorStopAt;
    }

    public String getFullCondition() {
        return fullCondition;
    }

    public String getLeftSideFullCondition() {
        return leftSideFullCondition;
    }

    public void setLeftSideFullCondition(String leftSideFullCondition) {
        this.leftSideFullCondition = leftSideFullCondition;
    }

    public String getRightSideFullCondition() {
        return rightSideFullCondition;
    }

    public void setRightSideFullCondition(String rightSideFullCondition) {
        this.rightSideFullCondition = rightSideFullCondition;
    }

    public void setLeftSideDataType(ConditionDataType leftSideDataType) {
        this.leftSideDataType = leftSideDataType;
    }

    public void setLeftSideValue(String leftSideValue) {
        this.leftSideValue = leftSideValue;
    }

    public void setRightSideDataType(ConditionDataType rightSideDataType) {
        this.rightSideDataType = rightSideDataType;
    }

    public void setRightSideValue(String rightSideValue) {
        this.rightSideValue = rightSideValue;
    }

    public void setOperator(String operator) {
        this.operator = operator;
    }

    public void setFullCondition(String fullCondition) {
        this.fullCondition = fullCondition;
    }

    public void setLeftSideNumberValue(double leftSideNumberValue) {
        this.leftSideNumberValue = leftSideNumberValue;
    }

    public void setRightSideNumberValue(double rightSideNumberValue) {
        this.rightSideNumberValue = rightSideNumberValue;
    }

    public static boolean isOrUsed(List<ConditionItem> conditions) {
        for (ConditionItem item : conditions) {
            if ((item.getRightLogicalOperator() != null && item.getRightLogicalOperator().equals("OR")) || (item.getLeftLogicalOperator() != null && item.getLeftLogicalOperator().equals("OR"))) {
                return true;
            }
        }
        return false;
    }

    public int getLeftLogicalOperatorStopAt() {
        return leftLogicalOperatorStopAt;
    }

    @Override
    public String toString() {
        return "ConditionItem{" +
                "\n\tleftSideDataType=" + leftSideDataType +
                "\n\t, leftSideValue='" + leftSideValue + '\'' +
                "\n\t, rightSideDataType=" + rightSideDataType +
                "\n\t, rightSideValue='" + rightSideValue + '\'' +
                "\n\t, operator='" + operator + '\'' +
                "\n\t, fullCondition='" + fullCondition + '\'' +
                "\n\t, leftSideFullCondition='" + leftSideFullCondition + '\'' +
                "\n\t, rightSideFullCondition='" + rightSideFullCondition + '\'' +
                "\n\t, leftSideNumberValue=" + leftSideNumberValue +
                "\n\t, rightSideNumberValue=" + rightSideNumberValue +
                "\n\t, startAt=" + startAt +
                "\n\t, stopAt=" + stopAt +
                "\n\t, leftLogicalOperatorStartAt=" + leftLogicalOperatorStartAt +
                "\n\t, leftLogicalOperatorStopAt=" + leftLogicalOperatorStopAt +
                "\n\t, rightLogicalOperatorStartAt=" + rightLogicalOperatorStartAt +
                "\n\t, rightLogicalOperatorStopAt=" + rightLogicalOperatorStopAt +
                "\n\t, leftLogicalOperator='" + leftLogicalOperator + '\'' +
                "\n\t, rightLogicalOperator='" + rightLogicalOperator + '\'' +
                "\n\t, betweenLeftCondition='" + betweenLeftCondition + '\'' +
                "\n\t, betweenRightCondition='" + betweenRightCondition + '\'' +
                "\n}";
    }
}

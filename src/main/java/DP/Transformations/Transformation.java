package DP.Transformations;

import DP.Database.ConditionItem;
import DP.Exceptions.UnnecessaryStatementException;

public class Transformation {
    String inputQuery;
    String outputQuery;
    String message;
    Action action;
    boolean changed;
    OperatorTransformation operatorTransformation;

    public Transformation() {
    }

    public Transformation(String inputQuery, String outputQuery, String message, Action action, boolean changed, OperatorTransformation operatorTransformation) {
        this.inputQuery = inputQuery;
        this.outputQuery = outputQuery;
        this.message = message;
        this.action = action;
        this.changed = changed;
        this.operatorTransformation = operatorTransformation;
    }

    public static Query addNewTransformationBasedOnLogicalOperator(Query query, ConditionItem condition, int conditionSize, Action action, String message) {
        if ((condition.getLeftLogicalOperator() != null && condition.getLeftLogicalOperator().equals("OR")) || (condition.getRightLogicalOperator() != null && condition.getRightLogicalOperator().equals("OR"))) {
            query.addTransformation(new Transformation(query.getCurrentQuery(),
                    query.getCurrentQuery(),
                    message + " " + UnnecessaryStatementException.messageConditionIsAlwaysTrue,
                    action,
                    false,
                    null
            ));
            return query;
        }

        String newQuery;

        if (conditionSize == 1) {
            newQuery = (query.getCurrentQuery().substring(0, (query.getCurrentQuery().substring(0, condition.getStartAt()).lastIndexOf("WHERE"))) + query.getCurrentQuery().substring(condition.getStopAt()).trim()).trim();
        } else {
            if (condition.getRightLogicalOperator() != null) {
                newQuery = (query.getCurrentQuery().substring(0, condition.getStartAt()) + query.getCurrentQuery().substring(condition.getRightLogicalOperatorStopAt() + 2)).trim();
            } else if (condition.getLeftLogicalOperator() != null) {
                newQuery = (query.getCurrentQuery().substring(0, condition.getLeftLogicalOperatorStartAt()) + query.getCurrentQuery().substring(condition.getStopAt())).trim();
            } else {
                newQuery = (query.getCurrentQuery().substring(0, condition.getStartAt()) + query.getCurrentQuery().substring(condition.getStopAt())).trim();
            }
        }
        query.addTransformation(new Transformation(query.getCurrentQuery(),
                newQuery,
                UnnecessaryStatementException.messageUnnecessaryStatement + " " + message,
                action,
                true,
                null
        ));
        return query;
    }

    public boolean isChanged() {
        return changed;
    }

    public void setChanged(boolean changed) {
        this.changed = changed;
    }

    public String getInputQuery() {
        return inputQuery;
    }

    public void setInputQuery(String inputQuery) {
        this.inputQuery = inputQuery;
    }

    public String getOutputQuery() {
        return outputQuery;
    }

    public void setOutputQuery(String outputQuery) {
        this.outputQuery = outputQuery;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public Action getAction() {
        return action;
    }

    public void setAction(Action action) {
        this.action = action;
    }

    public OperatorTransformation getoperatorTransformation() {
        return operatorTransformation;
    }

    public void setoperatorTransformation(OperatorTransformation operatorTransformation) {
        this.operatorTransformation = operatorTransformation;
    }

    @Override
    public String toString() {
        return "Transformation{" +
                "inputQuery='" + inputQuery + '\'' +
                ", outputQuery='" + outputQuery + '\'' +
                ", message='" + message + '\'' +
                ", action='" + action + '\'' +
                ", changed=" + changed +
                '}';
    }
}

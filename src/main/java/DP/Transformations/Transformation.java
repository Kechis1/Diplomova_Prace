package DP.Transformations;

import java.util.*;

public class Transformation {
    String inputQuery;
    String outputQuery;
    String message;
    Action action;
    boolean changed;
    List<OperatorTransformation> operatorTransformations;

    public Transformation() {
    }

    public Transformation(String inputQuery, String outputQuery, String message, Action action, boolean changed, List<OperatorTransformation> operatorTransformations) {
        this.inputQuery = inputQuery;
        this.outputQuery = outputQuery;
        this.message = message;
        this.action = action;
        this.changed = changed;
        this.operatorTransformations = operatorTransformations;
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

    public List<OperatorTransformation> getOperatorTransformations() {
        return operatorTransformations;
    }

    public void setOperatorTransformations(List<OperatorTransformation> operatorTransformations) {
        this.operatorTransformations = operatorTransformations;
    }

    public void addOperatorTransformation(OperatorTransformation operatorTransformation) {
        if (this.getOperatorTransformations() == null) {
            this.operatorTransformations = new ArrayList<>();
        }
        this.operatorTransformations.add(operatorTransformation);
    }

    public void addOperatorTransformation(List<OperatorTransformation> operatorTransformation) {
        if (this.getOperatorTransformations() == null) {
            this.operatorTransformations = new ArrayList<>();
        }
        this.operatorTransformations.addAll(operatorTransformation);
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

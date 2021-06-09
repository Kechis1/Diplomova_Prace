package DP.Transformations;
 
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

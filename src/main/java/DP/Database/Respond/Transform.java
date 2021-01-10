package DP.Database.Respond;

public class Transform {
    String inputQuery;
    String outputQuery;
    String message;
    String action;
    boolean changed;

    public Transform() {
    }

    public Transform(String inputQuery, String outputQuery, String message, String action, boolean changed) {
        this.inputQuery = inputQuery;
        this.outputQuery = outputQuery;
        this.message = message;
        this.action = action;
        this.changed = changed;
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

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }
}

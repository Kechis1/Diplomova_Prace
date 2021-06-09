package DP.Transformations;

public class OperatorTransformation {
    boolean ignored;
    String from;
    String to;

    public OperatorTransformation(boolean ignored, String from, String to) {
        this.ignored = ignored;
        this.from = from;
        this.to = to;
    }

    public boolean isIgnored() {
        return ignored;
    }

    public void setIgnored(boolean ignored) {
        this.ignored = ignored;
    }

    public String getFrom() {
        return from;
    }

    public void setFrom(String from) {
        this.from = from;
    }

    public String getTo() {
        return to;
    }

    public void setTo(String to) {
        this.to = to;
    }

    @Override
    public String toString() {
        return "OperatorTransformation{" +
                "ignored=" + ignored +
                ", from='" + from + '\'' +
                ", to='" + to + '\'' +
                '}';
    }
}

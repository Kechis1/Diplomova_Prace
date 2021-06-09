package DP.Transformations;

public class OperatorTransformation {
    boolean ignored;
    String subject;
    String from;
    String to;

    public OperatorTransformation(boolean ignored, String subject, String from, String to) {
        this.ignored = ignored;
        this.subject = subject;
        this.from = from;
        this.to = to;
    }

    public boolean isIgnored() {
        return ignored;
    }

    public void setIgnored(boolean ignored) {
        this.ignored = ignored;
    }

    public String getSubject() {
        return subject;
    }

    public void setSubject(String subject) {
        this.subject = subject;
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
}

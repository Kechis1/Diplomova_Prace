package DP.Exceptions;

public class UnnecessaryStatementException extends RuntimeException {
    public final static String messageCanBeRewrittenTo = "can be rewritten to";
    public final static String messageUnnecessaryStatement = "Unnecessary SQL statement:";

    public UnnecessaryStatementException(String message, String code) {
        super(message + code);
    }
}

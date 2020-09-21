package DP.Exceptions;

public class UnnecessaryStatementException extends RuntimeException {
    public final static String message = "Unnecessary SQL statement: ";

    public UnnecessaryStatementException(String code) {
        super(message + code);
    }
}

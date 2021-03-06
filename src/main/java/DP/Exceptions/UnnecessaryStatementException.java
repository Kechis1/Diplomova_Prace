package DP.Exceptions;

public class UnnecessaryStatementException extends Exception {
    public final static String messageCanBeRewrittenTo = "can be rewritten to";
    public final static String messageUnnecessaryStatement = "Unnecessary SQL statement:";
    public final static String messageAlwaysReturnsEmptySet = "Always returns empty set";
    public final static String messageUnnecessarySelectClause = "Unnecessary select clause:";
    public final static String messageConstant = "Possibly unnecessary constant:";
    public final static String messageDuplicateAttribute = "Duplicate attribute";
    public static String messageConditionIsAlwaysTrue = "condition is always true";

    public UnnecessaryStatementException(String message, String code) {
        super(message + code);
    }
}

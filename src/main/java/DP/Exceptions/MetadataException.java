package DP.Exceptions;

public class MetadataException extends Exception {
    private static final String message = "An error occurred while processing database metadata";

    public MetadataException() {
        super(message);
    }

    public MetadataException(String message) {
        super(MetadataException.message + ": " + message);
    }
}

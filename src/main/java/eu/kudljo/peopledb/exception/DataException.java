package eu.kudljo.peopledb.exception;

public class DataException extends RuntimeException {
    public DataException(String message) {
        super(message);
    }

    public DataException(String message, Throwable exception) {
        super(message, exception);
    }
}

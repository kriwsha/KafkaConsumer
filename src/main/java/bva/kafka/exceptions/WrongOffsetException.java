package bva.kafka.exceptions;

public class WrongOffsetException extends OffsetStorageException {

    public WrongOffsetException(Exception cause) {
        super(cause);
    }

    public WrongOffsetException(String message) {
        super(message);
    }

    public WrongOffsetException() {}
}

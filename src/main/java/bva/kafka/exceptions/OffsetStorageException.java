package bva.kafka.exceptions;

public class OffsetStorageException extends KafkaSourceException {

    public OffsetStorageException(Exception cause) {
        super(cause);
    }

    public OffsetStorageException(String message) {
        super(message);
    }

    public OffsetStorageException() {}
}

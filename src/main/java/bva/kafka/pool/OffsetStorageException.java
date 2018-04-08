package bva.kafka.pool;

public class OffsetStorageException extends KafkaSourceException {

    public OffsetStorageException(Exception ex) {
        super(ex);
    }

    public OffsetStorageException(String msg) {
        super(msg);
    }

    public OffsetStorageException() {}
}

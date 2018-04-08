package bva.kafka.pool;

public class WrongOffsetException extends OffsetStorageException {

    public WrongOffsetException(Exception ex) {
        super(ex);
    }

    public WrongOffsetException(String msg) {
        super(msg);
    }

    public WrongOffsetException() {}
}

package bva.kafka.pool;

public class KafkaSourceException extends Exception {

    public KafkaSourceException(Exception ex) {
        super(ex);
    }

    public KafkaSourceException(String msg) {
        super(msg);
    }

    public KafkaSourceException() {}
}

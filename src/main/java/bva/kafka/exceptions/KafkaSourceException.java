package bva.kafka.exceptions;

public class KafkaSourceException extends Exception {

    public KafkaSourceException(Exception cause) {
        super(cause);
    }

    public KafkaSourceException(String message) {
        super(message);
    }

    public KafkaSourceException() {}
}

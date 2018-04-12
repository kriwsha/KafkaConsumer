package bva.kafka.lib;

public interface OffsetStorage {
    void commitOffset();
    long getOffset();
    void close() throws Exception;
}

package bva.kafka.lib;

import org.apache.kafka.common.TopicPartition;

public interface OffsetStorage {
    void commitOffset(TopicPartition partition, long position);
    long getOffset();
    void close() throws Exception;
}

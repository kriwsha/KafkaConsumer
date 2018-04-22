package bva.kafka.lib;

import bva.kafka.exceptions.KafkaSourceException;
import org.apache.kafka.common.TopicPartition;

public interface OffsetStorage {
    void commitOffset(TopicPartition partition, long position) throws KafkaSourceException;
    long getOffset(String path) throws KafkaSourceException;
    void close() throws InterruptedException;
}

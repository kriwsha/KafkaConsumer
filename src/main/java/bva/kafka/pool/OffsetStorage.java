package bva.kafka.pool;

import org.apache.kafka.common.TopicPartition;

public interface OffsetStorage {
    void commitPosition (TopicPartition partition, long position) throws OffsetStorageException;
    long getPosition(TopicPartition partition) throws OffsetStorageException;
    void close();
}
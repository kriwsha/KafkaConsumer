package bva.kafka.lib;

import bva.kafka.exceptions.OffsetStorageException;
import org.apache.kafka.common.TopicPartition;

public interface IKafkaOffsetStorage {
    void commitPosition(TopicPartition topicPartition, long position) throws OffsetStorageException;
    long getPosition(TopicPartition topicPartition) throws OffsetStorageException;
}

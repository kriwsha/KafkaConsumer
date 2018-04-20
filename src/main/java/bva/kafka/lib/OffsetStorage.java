package bva.kafka.lib;

import org.apache.kafka.common.TopicPartition;

public interface OffsetStorage {
    void commitOffset(TopicPartition partition, long position) throws Exception;// TODO: 20.04.2018 поправить вид исключения
    long getOffset(String path) throws Exception;// TODO: 20.04.2018 поправить вид исключения
    void close() throws Exception;
}

package bva.kafka.pool;

import bva.kafka.exceptions.OffsetStorageException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.Collection;

public class OffsetsController {
    private static final Logger logger = LogManager.getLogger(OffsetsController.class);
    private Consumer<?,?> consumer;
    private IKafkaOffsetStorage storage;
    private CancellationToken cancellationToken;

    public OffsetsController (Consumer<?,?> consumer, IKafk aOffsetStorage storage, CancellationToken cancellationToken){
        this.consumer = consumer;
        this.storage = storage;
        this.cancellationToken = cancellationToken;
    }

    @Override
    public void onPartitionsRevoked (Collection<TopicPartition> partitions) {
        for (TopicPartition partition : partitions) {
            long position = consumer.position ( partition );
            try {
                storage.commitPosition ( partition, position );
                logger.info ( "onPartitionsRevoked: Сохранены оффсеты для {} к {}", partition, position );
            } catch (OffsetStorageException e) {
                errorHandler(e);
            }
        }
    }
    @Override
    public void onPartitionsAssigned (Collection<TopicPartition> partitions) {
        try {
            for (TopicPartition partition : partitions) {
                long position = storage.getPosition ( partition );
                consumer.seek ( partition, position );
                logger.info ( "onPartitionsAssigned: Восстановлены оффсеты для {} к {}", partition, position );
            }
        }
        catch (OffsetStorageException e) {
            errorHandler(e);
        }
    }

    public void errorHandler (Exception e) {
        e.printStackTrace ();
        logger.fatal ( "Ошибка при работе с оффсетами. Завершаем все потоки: {}", e.getMessage () );
        cancellationToken.setLastException ( e );
        cancellationToken.closeAll ();
    }
}

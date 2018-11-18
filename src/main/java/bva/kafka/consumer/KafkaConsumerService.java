package bva.kafka.consumer;

import bva.kafka.exceptions.KafkaSourceException;
import bva.kafka.ext.CancelToken;
import bva.kafka.ext.Props;
import bva.kafka.lib.ConsumerService;
import bva.kafka.lib.HandlerService;
import bva.kafka.lib.ServiceFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Component
public class KafkaConsumerService implements ConsumerService {
    private static final Logger logger = LogManager.getLogger(KafkaConsumerService.class);

    private CancelToken token = new CancelToken();
    private CountDownLatch latch = new CountDownLatch(0);
    private KafkaConsumer consumer;

    @Autowired
    private ConsumerConfiguration consumerConfiguration;

    @Autowired
    private HandlerServiceConfiguration handlerConfiguration;

    @Autowired
    private ServiceFactory serviceFactory;

    @Override
    public void start() {
        ExecutorService executor = null;

        try {
            logger.info(">>>> Start consumer");

            // TODO: 18.11.18 Здесь реализовать подключение внешнего jar-ника для обработки сообщений

            consumer = new KafkaConsumer<String, String>(Props.of(consumerConfiguration.getKafkaProps()));
            int partitionsCount = consumer.partitionsFor(consumerConfiguration.getTopic()).size();
            latch = new CountDownLatch(partitionsCount);

            executor = Executors.newFixedThreadPool(partitionsCount);
            List<Callable<Object>> threads = new ArrayList<>();

            for (int partitionNumber = 0; partitionNumber < partitionsCount; partitionNumber++) {
                threads.add(
                        Executors.callable(
                                new Worker(new TopicPartition(consumerConfiguration.getTopic(), partitionNumber)))
                );
            }

            executor.invokeAll(threads);
        } catch (Exception ex) {
            logger.info("Error while consumer execution", ex);
        } finally {
            if (executor != null) {
                executor.shutdown();
            }
        }

    }

    @Override
    public void stop() {
        token.cancel();
        try {
            while (latch.getCount() > 0) {
                Thread.sleep(2000);
            }
        } catch (InterruptedException ex) {
            System.out.println(ex.getMessage());
        } finally {
            logger.error("<<<< Stop consumer");
        }
    }

    class Worker implements Runnable {

        private static final int POLL_TIME = 1000;// TODO: 18.11.18 вынести в отдельный конфиг

        private TopicPartition topicPartition;
        private ZookeeperOffsetStorage offsetStorage;
        private HandlerService handlerService;

        Worker(TopicPartition topicPartition) throws IOException {
            this.topicPartition = topicPartition;
            this.offsetStorage = new ZookeeperOffsetStorage(
                    handlerConfiguration.getZkHosts(),
                    handlerConfiguration.getZkPath(),
                    topicPartition
            );
            this.handlerService = serviceFactory.getServiceById(handlerConfiguration.getServiceId());
        }

        @Override
        @SuppressWarnings("unchecked")
        public void run() {
            logger.info(String.format("Start execution on partition %d", topicPartition.partition()));

            while (!token.isCancel()) {
                ConsumerRecords<String, String> records = consumer.poll (POLL_TIME);
                for (ConsumerRecord<String, String> record : records) {
                    try {
                        String msg = record.value();
                        if (msg.isEmpty ()) {
                            throw new KafkaSourceException(String.format("Тело сообщения пусто :: партиция: %s; смещение: %s", record.partition(), record.offset()));
                        }
                        handlerService.handle(msg);
                        offsetStorage.commitOffset(record.offset());
                    } catch (KafkaSourceException ex) {
                        logger.error(ex);
                    }
                }
            }
            latch.countDown();
        }
    }
}

package bva.kafka.consumer;

import bva.kafka.ext.CancelToken;
import bva.kafka.ext.Props;
import bva.kafka.lib.*;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

@Component
public class KafkaConsumerService implements ConsumerService {
    private static final Logger logger = LogManager.getLogger(KafkaConsumerService.class);

    private CancelToken token = new CancelToken();
    private CountDownLatch latch = new CountDownLatch(0);

    @Autowired
    private ConsumerConfiguration consumerConfiguration;

    @Autowired
    private HandlerServiceConfiguration handlerConfiguration;

    @Autowired
    private ServiceFactory serviceFactory;

    @Override
    public void start() {
        try {
            logger.info(">>>> Start consumer");
            KafkaConsumer consumer = new KafkaConsumer<String, String>(Props.of(consumerConfiguration.getKafkaProps()));
            int partitionsCount = consumer.partitionsFor(consumerConfiguration.getTopic()).size();
            latch = new CountDownLatch(partitionsCount);
            ExecutorService executor = Executors.newFixedThreadPool(partitionsCount);
            List<Callable<Object>> threads = new ArrayList<>();
            for (int partitionNumber=0; partitionNumber<partitionsCount; partitionNumber++)
                threads.add(Executors.callable(new Worker(partitionNumber)));
            executor.invokeAll(threads);
        } catch (Exception ex) {
            logger.info("Error while consumer execution", ex);
        }

    }

    @Override
    public void stop() {
        token.cancel();
        try {
            while (latch.getCount() > 0)
                Thread.sleep(2000);
        } catch (InterruptedException ex) {
            System.out.println(ex.getMessage());
        }
        logger.error("<<<< Stop consumer");
    }

    class Worker implements Runnable {
        private int partitionNumber;

        Worker(int partitionNumber) {
            this.partitionNumber = partitionNumber;
        }

        @Override
        public void run() {
            logger.info(String.format("Start execution on partition %d", partitionNumber));
            HandlerService handlerService = serviceFactory.getServiceById(handlerConfiguration.getServiceId());

            while (!token.isCancel()) {

                // TODO: 22.04.2018 realize

            }
            latch.countDown();
        }
    }
}

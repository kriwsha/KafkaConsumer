package bva.kafka.consumer;

import bva.kafka.lib.*;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.*;

@Component
public class KafkaConsumerService implements ConsumerService {

    private CancelToken token = new CancelToken();
    private CountDownLatch latch = new CountDownLatch(0);

    @Autowired
    private ConsumerConfiguration configuration;

    @Autowired
    private ServiceFactory serviceFactory;

    @Override
    public void start() {
        try {
            System.out.println("Start consumer");
            KafkaConsumer consumer = new KafkaConsumer<String, String>(Props.of(configuration.getKafkaProps()));
            int partitionsCount = consumer.partitionsFor(configuration.getTopic()).size();
            latch = new CountDownLatch(partitionsCount);
            ExecutorService executor = Executors.newFixedThreadPool(partitionsCount);
            List<Callable<Object>> threads = new ArrayList<>();
            for (int partitionNumber=0; partitionNumber<partitionsCount; partitionNumber++)
                threads.add(Executors.callable(new Worker(partitionNumber)));
            executor.invokeAll(threads);
        } catch (Exception ex) {
            ex.printStackTrace();
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
        System.out.println("Close consumer");
    }

    class Worker implements Runnable {
        private int partitionNumber;

        Worker(int partitionNumber) {
            this.partitionNumber = partitionNumber;
        }

        @Override
        public void run() {
            System.out.println(String.format("Start execution on partition %d", partitionNumber));
            HandlerService handlerService = serviceFactory.getServiceById(configuration.getServiceId());

            while (!token.isCancel()) {

            }

            latch.countDown();
        }
    }
}

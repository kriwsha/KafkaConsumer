package bva.kafka.consumer;

import bva.kafka.lib.CancelToken;
import bva.kafka.lib.ConsumerService;
import bva.kafka.lib.HandlerService;
import bva.kafka.lib.Props;
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

    @Override
    public void start() {
        try {
            System.out.println("Start consumer");
            Properties kafkaProps = Props.of(configuration.getKafkaProps());
            KafkaConsumer consumer = new KafkaConsumer<String, String>(kafkaProps);
            int partitionsCount = consumer.partitionsFor(configuration.getTopic()).size();
            latch = new CountDownLatch(partitionsCount);
            ExecutorService executor = Executors.newFixedThreadPool(partitionsCount);
            List<Callable<Object>> threads = new ArrayList<>();
            for (int i=0; i<partitionsCount; i++)
                threads.add(Executors.callable(new Worker()));
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

        Worker() {

        }

        @Override
        public void run() {
            while (!token.isCancel()) {
                //handle
            }
            latch.countDown();
        }
    }
}

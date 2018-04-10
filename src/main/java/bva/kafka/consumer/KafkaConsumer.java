package bva.kafka.consumer;

import bva.kafka.lib.CancelToken;
import bva.kafka.lib.ConsumerService;
import bva.kafka.lib.HandlerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.CountDownLatch;

@Component
public class KafkaConsumer implements ConsumerService {

    private CancelToken token = new CancelToken();
    private CountDownLatch latch = new CountDownLatch(0);

    @Autowired
    private ConsumerConfiguration configuration;



    @Override
    public void start() {
        try {
            System.out.println("Start consumer");
            for (int i=0; i<10; i++) {
                System.out.println(i + " - Consumer");
                Thread.sleep(1000);
            }
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

        @Override
        public void run() {
            while (!token.isCancel()) {
                //handle
            }
            latch.countDown();
        }
    }
}

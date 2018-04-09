package bva.kafka.consumer;

import bva.kafka.lib.ConsumerService;
import bva.kafka.lib.HandlerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.CountDownLatch;

@Component
public class KafkaConsumer implements ConsumerService {

    @Autowired
    private ConsumerConfiguration configuration;

    private CountDownLatch latch;

    @Override
    public void start() {
        System.out.println("Start consumer");
    }

    @Override
    public void stop() {
        System.out.println("Close consumer");
    }
}

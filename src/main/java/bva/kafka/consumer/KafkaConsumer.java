package bva.kafka.consumer;

import bva.kafka.lib.ConsumerService;
import bva.kafka.lib.HandlerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class KafkaConsumer implements ConsumerService {

    @Autowired
    private HandlerService service;

    @Override
    public void start() {
        service.handle("");
    }
}

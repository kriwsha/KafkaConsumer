package bva.kafka.consumer;

import bva.kafka.lib.ConsumerService;
import bva.kafka.lib.HandlerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class KafkaConsumer implements ConsumerService {

//    @Autowired
//    private HandlerService service;

    @Autowired
    private ConsumerConfiguration configuration;

    @Override
    public void start() {
        Map<String, String> map1 = configuration.getKafkaProps();
        Map<String, String> map2 = configuration.getZkProps();
//        service.handle("");
        System.out.println();
    }
}

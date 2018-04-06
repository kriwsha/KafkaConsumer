package bva.kafka.consumer;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.Map;

@Configuration
@ConfigurationProperties(prefix = "kafka.props")
class ConsumerConfiguration {
    private Map<String, String> kafkaProps;

    public Map<String, String> getKafkaProps() {
        return kafkaProps;
    }
}

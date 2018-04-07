package bva.kafka.consumer;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
@ConfigurationProperties(prefix = "cons")
class ConsumerConfiguration {
    private Map<String, String> kafkaProps;
    private Map<String, String> zkProps;

    public Map<String, String> getKafkaProps() {
        return kafkaProps;
    }

    public void setKafkaProps(Map<String, String> kafkaProps) {
        this.kafkaProps = kafkaProps;
    }

    public Map<String, String> getZkProps() {
        return zkProps;
    }

    public void setZkProps(Map<String, String> zkProps) {
        this.zkProps = zkProps;
    }
}

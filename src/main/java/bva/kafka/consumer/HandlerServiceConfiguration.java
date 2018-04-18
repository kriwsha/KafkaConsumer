package bva.kafka.consumer;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
@ConfigurationProperties(prefix = "handler")
public class HandlerServiceConfiguration {
    private Map<String, String> zkProps;

    private String serviceId;

    public Map<String, String> getZkProps() {
        return zkProps;
    }

    public void setZkProps(Map<String, String> zkProps) {
        this.zkProps = zkProps;
    }

    public String getServiceId() {
        return serviceId;
    }

    public void setServiceId(String serviceId) {
        this.serviceId = serviceId;
    }
}

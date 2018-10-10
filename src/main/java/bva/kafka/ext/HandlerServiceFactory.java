package bva.kafka.ext;

import bva.kafka.lib.HandlerService;
import bva.kafka.lib.ServiceFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Arrays;

public class HandlerServiceFactory implements ServiceFactory {
    @Autowired
    HandlerService[] services;

    @Override
    public HandlerService getServiceById(String serviceId) {
        return Arrays.stream(services).filter(s -> s.getId().equals(serviceId)).findFirst().get();
    }
}

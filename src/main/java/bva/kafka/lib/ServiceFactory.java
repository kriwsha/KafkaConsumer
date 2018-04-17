package bva.kafka.lib;

public interface ServiceFactory {
    HandlerService getServiceById(String serviceId);
}

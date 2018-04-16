package bva.kafka.lib;

import java.util.concurrent.locks.ReentrantLock;

public interface HandlerService {
    ReentrantLock locker = new ReentrantLock();

    default void handle(String message) {
        locker.lock();
        handleMessage(message);
        locker.unlock();
    }

    void handleMessage(String message);
}

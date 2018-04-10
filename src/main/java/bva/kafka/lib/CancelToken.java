package bva.kafka.lib;

import java.util.concurrent.atomic.AtomicBoolean;

public class CancelToken {
    private AtomicBoolean cancel = new AtomicBoolean(false);

    public boolean isCancel() {
        return cancel.get();
    }

    public void cancel() {
        cancel.set(true);
    }
}

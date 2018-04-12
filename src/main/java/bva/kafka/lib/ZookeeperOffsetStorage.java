package bva.kafka.lib;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

public class ZookeeperOffsetStorage implements OffsetStorage{
    private ZooKeeper zk;
    private String path;
    private final int ZOO_TIMEOUT = 20000;

    public ZookeeperOffsetStorage(String hosts, String path) throws IOException {
        this.path = path;
        this.zk = new ZooKeeper(hosts, ZOO_TIMEOUT,
                (WatchedEvent watchedEvent) -> {
                    System.out.println("process");
                });
    }

    public void commitOffset() {

    }

    @Override
    public long getOffset() {
        return 0;
    }

    public void close() throws InterruptedException {
        zk.close();
    }
}

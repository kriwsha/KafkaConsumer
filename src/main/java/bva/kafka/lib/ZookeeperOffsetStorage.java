package bva.kafka.lib;

import org.apache.kafka.common.TopicPartition;
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

    @Override
    public void commitOffset(TopicPartition partition, long position) {
        String fullPath = createFullPath(partition.topic(), partition.partition());
        byte[] positionByteView = String.valueOf(position).getBytes();

    }

    @Override
    public long getOffset() {
        return 0;
    }

    @Override
    public void close() throws InterruptedException {
        zk.close();
    }

    private String createFullPath(String topik, int partition) {
        String pathToTopik = path.endsWith("/")? path : String.format("%s/", path);
        return String.format("%s%s/%s", pathToTopik, topik, partition);
    }
}

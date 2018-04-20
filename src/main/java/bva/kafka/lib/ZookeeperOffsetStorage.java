package bva.kafka.lib;

import org.apache.kafka.common.TopicPartition;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

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
    public void commitOffset(TopicPartition partition, long position) throws Exception {// TODO: 20.04.2018 поправить исключение здесь и в интерфейсе
        String fullPath = createFullPath(partition.topic(), partition.partition());
        try {
            long previousPosition = getOffset(fullPath);
            if (previousPosition < position) {
                updatePosition(path, position);
            } else {
                throw new Exception(String.format("Bad offset in: %s, current: %d, new: %d", fullPath, position, previousPosition)); // TODO: 20.04.2018 изменить на WrongOffsetException
            }
        } catch (KeeperException ex) {
            throw new Exception(); // TODO: 20.04.2018 изменить на OffsetStorageException
        } catch (InterruptedException ex) {
            ex.printStackTrace();// TODO: 20.04.2018 change to logging
        }
    }

    @Override
    public long getOffset(String fullPath) throws Exception {// TODO: 20.04.2018 поправить
        try {
            Stat existStat = zk.exists(fullPath, true);
            byte[] bytesPosition = zk.getData(fullPath, true, existStat);
            return Long.parseLong(new String(bytesPosition, StandardCharsets.UTF_8));
        } catch (KeeperException | InterruptedException ex) {
            throw new Exception();  // TODO: 20.04.2018 добавить исключение OffsetStorageException
        }
    }

    @Override
    public void close() throws InterruptedException {
        zk.close();
    }

    private void updatePosition(String path, long position) throws KeeperException, InterruptedException{
        byte[] currentPositionByteView = String.valueOf(position).getBytes();
        int version = zk.exists(path, true).getVersion();
        zk.setData(path, currentPositionByteView, version);
    }

    private String createFullPath(String topik, int partition) {
        String pathToTopik = path.endsWith("/")? path : String.format("%s/", path);
        return String.format("%s%s/%s", pathToTopik, topik, partition);
    }
}

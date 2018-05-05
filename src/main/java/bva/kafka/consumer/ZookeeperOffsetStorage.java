package bva.kafka.consumer;

import bva.kafka.exceptions.OffsetStorageException;
import bva.kafka.exceptions.WrongOffsetException;
import bva.kafka.lib.OffsetStorage;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class ZookeeperOffsetStorage implements OffsetStorage {
    private static final Logger logger = LogManager.getLogger(ZookeeperOffsetStorage.class);

    private ZooKeeper zk;
    private String path;
    private TopicPartition topicPartition;
    private String offsetStoragePath;
    private final int ZOO_TIMEOUT = 20000;

    public ZookeeperOffsetStorage(String hosts, String path, TopicPartition topicPartition) throws IOException {
        this.path = path;
        this.topicPartition = topicPartition;
        this.offsetStoragePath = createFullPath(topicPartition.topic(), topicPartition.partition());
        this.zk = new ZooKeeper(hosts, ZOO_TIMEOUT,
                (WatchedEvent watchedEvent) -> {
                    logger.debug("Start Zookeeper offset storage process");
                });
    }

    @Override
    public void commitOffset(long position) throws OffsetStorageException {
        logger.debug(String.format("Commit offset:: partition: %s; position: %s", topicPartition.partition(), position));
        try {
            long previousPosition = getOffset(offsetStoragePath);
            if (previousPosition < position) {
                updatePosition(path, position);
            } else {
                throw new WrongOffsetException(String.format("Bad offset in: %s, current: %d, new: %d", offsetStoragePath, position, previousPosition)); // TODO: 20.04.2018 изменить на WrongOffsetException
            }
        } catch (KeeperException ex) {
            throw new OffsetStorageException(ex);
        } catch (InterruptedException ex) {
            logger.error("Error while offset committing", ex);
        }
    }

    @Override
    public long getOffset(String fullPath) throws OffsetStorageException {
        try {
            Stat existStat = zk.exists(fullPath, true);
            byte[] bytesPosition = zk.getData(fullPath, true, existStat);
            return Long.parseLong(new String(bytesPosition, StandardCharsets.UTF_8));
        } catch (KeeperException | InterruptedException ex) {
            throw new OffsetStorageException(ex);
        }
    }

    @Override
    public void close() throws InterruptedException {
        logger.debug("Close Zookeeper offset storage");
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

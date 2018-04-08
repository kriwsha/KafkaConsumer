package bva.kafka.pool;

import org.apache.kafka.common.TopicPartition;
import org.apache.zookeeper.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;

public class ZookeeperOffsetStorage implements OffsetStorage {
    private ZooKeeper zk;
    private String hosts;
    private String path;
    private final int ZOO_TIMEOUT = 20000;

    public  ZookeeperOffsetStorage (String hosts, String path){
        this.hosts = hosts;
        this.path = path;
    }

    private final CountDownLatch connectedSignal = new CountDownLatch(1);

    public void connect() throws IOException,InterruptedException {
        zk = new ZooKeeper(hosts,ZOO_TIMEOUT,
                new Watcher () {
                    public void process(WatchedEvent we) {
                        if (we.getState() == Event.KeeperState.SyncConnected) {
                            connectedSignal.countDown();
                        }
                    }
                });
        connectedSignal.await();
    }

    public void close() {
        try {
            zk.close();
        } catch (InterruptedException e) {
            e.printStackTrace ( );
        }
    }

    public void createNode(String path) throws KeeperException,InterruptedException {
        zk.create(path, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    @Override
    public void commitPosition (TopicPartition partition, long position) throws OffsetStorageException {
        String path = getFullPath(partition.topic (), partition.partition ());
        byte[] bytes = String.valueOf ( position).getBytes ();
        try {
            long prevPosition = getPosition(partition);

            if (position>prevPosition)
                update ( path, bytes );
            else if (position<prevPosition){
                String errorMessage = "BAD OFFSETS:" + path + " current: " + prevPosition + " new: " + position;
                throw new WrongOffsetException(errorMessage);
            }
        }
        catch (KeeperException e) {
            throw new OffsetStorageException(e);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public long getPosition(TopicPartition partition) throws OffsetStorageException {
        try {
            String path = getFullPath (partition.topic(), partition.partition());
            byte[] bytes = zk.getData ( path, true, zk.exists ( path, true ) );
            return Long.parseLong (new String ( bytes, StandardCharsets.UTF_8 ) );
        }
        catch (KeeperException | InterruptedException e ) {
            throw new OffsetStorageException(e);
        }
    }

    private boolean nodeExists(String path) throws KeeperException,InterruptedException {
        return zk.exists(path, true)!=null;
    }

    private void update(String path, byte[] data) throws KeeperException,InterruptedException {
        zk.setData(path, data, zk.exists(path,true).getVersion());
    }

    private String getFullPath (String topic, long partition){
        if (!path.endsWith ( "/" )) path+="/";
        return path + topic  + "/" + partition;
    }
}

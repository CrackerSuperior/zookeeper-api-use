package com.cracker.zookeeper.api.connect;

import lombok.Getter;
import lombok.Setter;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

@Getter
@Setter
public class ZooKeeperConnection {
    
    private ZooKeeper zooKeeper;
    
    private final CountDownLatch connectedSignal = new CountDownLatch(1);
    
    /**
     * Connect zookeeper ensemble.
     * @param host zookeeper address
     * @return zookeeper instance
     * @throws IOException IOException
     * @throws InterruptedException if the thread is interrupted
     */
    public ZooKeeper connect(final String host) throws IOException, InterruptedException {
        zooKeeper = new ZooKeeper(host, 5000, watchedEvent -> {
            if (watchedEvent.getState() == KeeperState.SyncConnected) {
                connectedSignal.countDown();
            }
        });
        connectedSignal.await();
        return zooKeeper;
    }
    
    /**
     * Disconnect from zookeeper server.
     * @throws InterruptedException if the thread is interrupted
     */
    public void close() throws InterruptedException {
        zooKeeper.close();
    }
}

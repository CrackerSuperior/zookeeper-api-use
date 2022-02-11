package com.cracker.zookeeper.api.node.create;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

@Setter
@Getter
@AllArgsConstructor
public class ZooKeeperNodeCreate {
    
    private ZooKeeper zooKeeper;
    
    /**
     * Create ZNode in zookeeper ensemble.
     * @param path ZNode path
     * @param data ZNode data
     * @throws InterruptedException if the thread is interrupted
     * @throws KeeperException zookeeper exception
     */
    public void create(final String path, final byte[] data) throws InterruptedException, KeeperException {
        zooKeeper.create(path, data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }
}

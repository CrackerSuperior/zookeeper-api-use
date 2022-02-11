package com.cracker.zookeeper.api.node;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.util.List;

@Setter
@Getter
@AllArgsConstructor
public class ZooKeeperNode {
    
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
    
    /**
     * Check existence of ZNode and its status, if ZNode is available.
     * @param path ZNode path
     * @return ZNode status
     * @throws InterruptedException if the thread is interrupted
     * @throws KeeperException zookeeper exception
     */
    public Stat exists(final String path) throws InterruptedException, KeeperException {
        return zooKeeper.exists(path, true);
    }
    
    /**
     * Get the data attached in a specified ZNode and its status.
     * @param path ZNode path
     * @param watcher callback function of type Watcher. The ZooKeeper ensemble will notify through the Watcher callback when the data of the specified ZNode changes. This is one-time notification
     * @param stat returns the metadata of a ZNode.
     * @return ZNode data
     * @throws InterruptedException if the thread is interrupted
     * @throws KeeperException zookeeper exception
     */
    public byte[] getData(final String path, final Watcher watcher, final Stat stat) throws InterruptedException, KeeperException {
        return zooKeeper.getData(path, watcher, stat);
    }
    
    /**
     * Update the data in a ZNode. Similar to getData but without watcher.
     * @param path ZNode path
     * @param data data to store in a specified ZNode path
     * @throws InterruptedException if the thread is interrupted
     * @throws KeeperException zookeeper exception
     */
    public void update(final String path, final byte[] data) throws InterruptedException, KeeperException {
        zooKeeper.setData(path, data, exists(path).getVersion());
    }
    
    /**
     * Get all the sub-node of a particular ZNode.
     * @param path ZNode path
     * @param watcher callback function of type “Watcher”. The ZooKeeper ensemble will notify when the specified ZNode gets deleted or a child under the ZNode gets created or deleted.
     * 
     * <p></p>This is a one-time notification.
     * @return get all the sub-node of a particular ZNode
     * @throws InterruptedException if the thread is interrupted
     * @throws KeeperException zookeeper exception
     */
    public List<String> getChildren(final String path, final Watcher watcher) throws InterruptedException, KeeperException {
        return null == watcher ? zooKeeper.getChildren(path, false) : zooKeeper.getChildren(path, watcher);
    }
    
    /**
     * Delete a specified ZNode.
     * @param path ZNode path
     * @throws InterruptedException if the thread is interrupted
     * @throws KeeperException zookeeper exception
     */
    public void delete(final String path) throws InterruptedException, KeeperException {
        zooKeeper.delete(path, exists(path).getVersion());
    }
}

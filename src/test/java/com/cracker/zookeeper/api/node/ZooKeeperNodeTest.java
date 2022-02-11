package com.cracker.zookeeper.api.node;

import com.cracker.zookeeper.api.connect.ZooKeeperConnection;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Test;

public class ZooKeeperNodeTest {
    
    @Test
    public void createNode() {
        String path = "/MyFirstZNode";
        byte[] data = "My first zookeeper app".getBytes();
        try {
            ZooKeeperConnection zooKeeperConnection = new ZooKeeperConnection();
            ZooKeeper zooKeeper = zooKeeperConnection.connect("localhost");
            ZooKeeperNode zooKeeperNode = new ZooKeeperNode(zooKeeper);
            zooKeeperNode.create(path, data);
            zooKeeperConnection.close();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
}

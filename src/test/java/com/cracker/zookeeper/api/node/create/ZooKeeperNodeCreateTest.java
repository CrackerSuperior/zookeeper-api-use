package com.cracker.zookeeper.api.node.create;

import com.cracker.zookeeper.api.connect.ZooKeeperConnection;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Test;

public class ZooKeeperNodeCreateTest {
    
    @Test
    public void createNode() {
        String path = "/MyFirstZNode";
        byte[] data = "My first zookeeper app".getBytes();
        try {
            ZooKeeperConnection zooKeeperConnection = new ZooKeeperConnection();
            ZooKeeper zooKeeper = zooKeeperConnection.connect("localhost");
            ZooKeeperNodeCreate zooKeeperNodeCreate = new ZooKeeperNodeCreate(zooKeeper);
            zooKeeperNodeCreate.create(path, data);
            zooKeeperConnection.close();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
}

package com.cracker.zookeeper.api.connect;

import org.apache.zookeeper.ZooKeeper;
import org.junit.Test;

public class ZooKeeperConnectionTest {
    
    @Test
    public void connect() {
        try {
            ZooKeeperConnection zooKeeperConnection = new ZooKeeperConnection();
            ZooKeeper zooKeeper = zooKeeperConnection.connect("localhost");
            System.out.println(zooKeeper.getState());
            zooKeeperConnection.close();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
}

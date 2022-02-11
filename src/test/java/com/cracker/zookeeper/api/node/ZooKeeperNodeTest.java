package com.cracker.zookeeper.api.node;

import com.cracker.zookeeper.api.connect.ZooKeeperConnection;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;

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
    
    @Test
    public void existsNode() {
        String path = "/MyFirstZNode";
        try {
            ZooKeeperConnection zooKeeperConnection = new ZooKeeperConnection();
            ZooKeeper zooKeeper = zooKeeperConnection.connect("localhost");
            ZooKeeperNode zooKeeperNode = new ZooKeeperNode(zooKeeper);
            Stat stat = zooKeeperNode.exists(path);
            if(stat != null) {
                System.out.println("Node exists and the node version is " + stat.getVersion());
            } else {
                System.out.println("Node does not exists");
            }
            zooKeeperConnection.close();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
    
    @Test
    public void getNodeData() {
        String path = "/MyFirstZNode";
        CountDownLatch connectedSignal = new CountDownLatch(1);
        try {
            ZooKeeperConnection zooKeeperConnection = new ZooKeeperConnection();
            ZooKeeper zooKeeper = zooKeeperConnection.connect("localhost");
            ZooKeeperNode zooKeeperNode = new ZooKeeperNode(zooKeeper);
            if (zooKeeperNode.exists(path) != null) {
                byte[] bytes = zooKeeperNode.getData(path, watchedEvent -> {
                    if (watchedEvent.getType() == EventType.None) {
                        if (watchedEvent.getState() == KeeperState.Expired) {
                            connectedSignal.countDown();
                        }
                    } else {
                        try {
                            byte[] tempBytes = zooKeeper.getData(path, false, null);
                            String data = new String(tempBytes, StandardCharsets.UTF_8);
                            System.out.println("data1" + data);
                            connectedSignal.countDown();
                        } catch (Exception e) {
                            System.out.println(e.getMessage());
                        }
                    }
                }, null);
                connectedSignal.countDown();
                String data = new String(bytes, StandardCharsets.UTF_8);
                System.out.println("data2" + data);
                connectedSignal.await();
            } else {
                System.out.println("Node does not exists");
            }
            zooKeeperConnection.close();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
}

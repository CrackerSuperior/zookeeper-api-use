package com.cracker.zookeeper.api.node;

import com.cracker.zookeeper.api.connect.ZooKeeperConnection;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class ZooKeeperNodeTest {
    
    ZooKeeperConnection zooKeeperConnection;
    ZooKeeper zooKeeper;
    ZooKeeperNode zooKeeperNode;
    
    private void init() throws IOException, InterruptedException {
        zooKeeperConnection = new ZooKeeperConnection();
        zooKeeper = zooKeeperConnection.connect("localhost");
        zooKeeperNode = new ZooKeeperNode(zooKeeper);
    }
    
    private void close() throws InterruptedException {
        zooKeeperConnection.close();
    }
    
    @Test
    public void createNode() {
        String path = "/MyFirstZNode";
        byte[] data = "My first zookeeper app".getBytes();
        try {
            init();
            zooKeeperNode.create(path, data);
            close();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
    
    @Test
    public void existsNode() {
        String path = "/MyFirstZNode";
        try {
            init();
            Stat stat = zooKeeperNode.exists(path);
            if(stat != null) {
                System.out.println("Node exists and the node version is " + stat.getVersion());
            } else {
                System.out.println("Node does not exists");
            }
            close();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
    
    /**
     * And the application will wait for further notification from the ZooKeeper ensemble.
     * 
     * <p>And it's disposable.
     */
    @Test
    public void getNodeData() {
        String path = "/MyFirstZNode";
        CountDownLatch connectedSignal = new CountDownLatch(1);
        try {
            init();
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
                            System.out.println("data1 = " + data);
                            connectedSignal.countDown();
                        } catch (Exception e) {
                            System.out.println(e.getMessage());
                        }
                    }
                }, null);
                String data = new String(bytes, StandardCharsets.UTF_8);
                System.out.println("data2 = " + data);
                connectedSignal.await();
            } else {
                System.out.println("Node does not exists");
            }
            close();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
    
    @Test
    public void updateNode() {
        String path = "/MyFirstZNode";
        byte[] data = "Success".getBytes();
        try {
            init();
            zooKeeperNode.update(path,data);
            close();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
    
    @Test
    public void getChildrenNode() {
        String path = "/MyFirstZNode";
        addNode("/MyFirstZNode/MyFirstSubNode", "Hi".getBytes());
        addNode("/MyFirstZNode/MySecondSubNode", "Hello".getBytes());
        try {
            init();
            if (zooKeeperNode.exists(path) != null) {
                List<String> children = zooKeeperNode.getChildren(path, null);
                children.forEach(System.out::println);
            } else {
                System.out.println("Node does not exists");
            }
            close();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
    
    private void addNode(final String path, final byte[] data) {
        try {
            init();
            zooKeeperNode.create(path, data);
            close();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
    
    @Test
    public void deleteNode() {
        String path = "/MyFirstZNode/MyFirstSubNode";
        try {
            init();
            zooKeeperNode.delete(path);
            close();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
}

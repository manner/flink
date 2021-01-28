package org.apache.flink.connector.hbase.source.standalone;

import org.apache.flink.connector.hbase.source.reader.HBaseEvent;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.ipc.FifoRpcScheduler;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.ipc.RpcServerFactory;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

/** Test Hbase Consumer. */
public class HBaseConsumer {

    private static final String subscriptionName = "cdc";
    private static Configuration hbaseConf;
    private static ZooKeeper zooKeeper;
    private static String table;
    private final ReplicationTargetServer server;

    public HBaseConsumer(Configuration hbaseConf, String table)
            throws ParserConfigurationException, SAXException, IOException, KeeperException,
                    InterruptedException {

        this.hbaseConf = hbaseConf;
        this.table = table;

        // Setup
        zooKeeper = connectZooKeeper();
        server = createServer();

        tryReplication();
    }

    private static String getBaseString() {
        return hbaseConf.get("hbasesep.zookeeper.znode.parent", "/hbase");
    }

    private static String getPort() {
        return hbaseConf.get("hbase.zookeeper.property.clientPort");
    }

    private static int findFreePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static void createZKPath(
            final String path, byte[] data, List<ACL> acl, CreateMode createMode)
            throws KeeperException, InterruptedException {
        createZKPath(path, data, acl, createMode, 5);
    }

    private static void createZKPath(
            final String path, byte[] data, List<ACL> acl, CreateMode createMode, int retries)
            throws KeeperException, InterruptedException {
        try {
            if (zooKeeper.exists(path, false) == null) {
                zooKeeper.create(path, data, acl, createMode);
            }
        } catch (KeeperException e) {
            System.err.println("Error creating ZK path: " + e.getMessage());
            if (retries > 0) {
                System.err.printf("Retry ... (%d retries left)", retries);
                createZKPath(path, data, acl, createMode, retries - 1);
            } else {
                System.err.println("Abort");
                throw e;
            }
        }
    }

    public HBaseEvent next() {
        return server.next();
    }

    private ZooKeeper connectZooKeeper() throws IOException {
        ZooKeeper zooKeeper =
                new ZooKeeper(
                        "localhost:" + getPort(),
                        20000,
                        new Watcher() {

                            @Override
                            public void process(WatchedEvent event) {
                                System.out.println("Watcher processed: " + event);
                            }
                        });
        while ((ZooKeeper.States.CONNECTED).equals(zooKeeper.getState())) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
                System.err.println("Cannot connect to Zookeeper");
                return null;
            }
        }

        System.out.println("Connected to Zookeeper");

        return zooKeeper;
    }

    private void tryReplication() {
        try (Connection connection = ConnectionFactory.createConnection(hbaseConf);
                Admin admin = connection.getAdmin(); ) {
            admin.listReplicationPeers().stream()
                    .filter(peer -> peer.getPeerId().equals("flink_cdc"))
                    .forEach(
                            peer -> {
                                try {
                                    admin.removeReplicationPeer(peer.getPeerId());
                                } catch (IOException e) {
                                    e.printStackTrace();
                                }
                            });

            HashMap tableMap = new HashMap<>();
            tableMap.put(TableName.valueOf(table), null);
            ReplicationPeerConfig peerConfig =
                    ReplicationPeerConfig.newBuilder()
                            .setClusterKey(
                                    "localhost:"
                                            + getPort()
                                            + ":"
                                            + getBaseString()
                                            + "/"
                                            + subscriptionName)
                            .setReplicateAllUserTables(false)
                            .setTableCFsMap(tableMap)
                            .build();
            admin.addReplicationPeer("flink_cdc", peerConfig);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private ReplicationTargetServer createServer()
            throws KeeperException, InterruptedException, ZooKeeperConnectionException,
                    IOException {
        ReplicationTargetServer server = new ReplicationTargetServer();

        String hostName = "localhost";
        InetSocketAddress initialIsa = new InetSocketAddress(hostName, 0);
        String name = "regionserver/" + initialIsa.toString();

        RpcServer.BlockingServiceAndInterface bsai =
                new RpcServer.BlockingServiceAndInterface(
                        AdminProtos.AdminService.newReflectiveBlockingService(server),
                        org.apache.hadoop.hbase.protobuf.generated.AdminProtos.AdminService
                                .BlockingInterface.class);
        RpcServer rpcServer =
                RpcServerFactory.createRpcServer(
                        server,
                        name,
                        Arrays.asList(bsai),
                        initialIsa,
                        hbaseConf,
                        new FifoRpcScheduler(
                                hbaseConf,
                                hbaseConf.getInt("hbase.regionserver.handler.count", 10)));

        UUID uuid = UUID.nameUUIDFromBytes(Bytes.toBytes(subscriptionName));
        createZKPath(
                getBaseString() + "/" + subscriptionName,
                null,
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);
        createZKPath(
                getBaseString() + "/" + subscriptionName + "/rs",
                null,
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);

        createZKPath(
                getBaseString() + "/" + subscriptionName + "/hbaseid",
                Bytes.toBytes(uuid.toString()),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);

        ServerName serverName =
                ServerName.valueOf(
                        hostName,
                        rpcServer.getListenerAddress().getPort(),
                        System.currentTimeMillis());
        ZKWatcher zkWatcher = new ZKWatcher(hbaseConf, serverName.toString(), null);
        rpcServer.start();
        zooKeeper.create(
                getBaseString() + "/" + subscriptionName + "/rs/" + serverName.getServerName(),
                null,
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL);

        return server;
    }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.hbase.source.hbaseendpoint;

import org.apache.flink.connector.hbase.source.reader.HBaseEvent;
import org.apache.flink.connector.hbase.util.HBaseConfigurationUtil;

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
import org.apache.hadoop.hbase.replication.ReplicationPeerDescription;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

/** Consumer of HBase WAL edits. */
public class HBaseConsumer {

    private final String clusterKey;
    /** The id under which the replication target is made known to the source cluster. */
    private final String replicationPeerId;

    private Configuration hbaseConf;
    private RecoverableZooKeeper zooKeeper;
    private final ReplicationTargetServer server;
    private RpcServer rpcServer;

    private boolean isRunning = false;

    public HBaseConsumer(byte[] serializedConfig)
            throws IOException, KeeperException, InterruptedException {
        this(HBaseConfigurationUtil.deserializeConfiguration(serializedConfig, null));
    }

    public HBaseConsumer(Configuration hbaseConf)
            throws InterruptedException, KeeperException, IOException {
        this(UUID.randomUUID().toString().substring(0, 5), hbaseConf);
    }

    public HBaseConsumer(String peerId, Configuration hbaseConf)
            throws IOException, KeeperException, InterruptedException {
        this.hbaseConf = hbaseConf;
        this.clusterKey = peerId + "_clusterKey";
        this.replicationPeerId = peerId;

        // Setup
        zooKeeper = connectZooKeeper();
        server = createServer();
    }

    private String getBaseString() {
        return hbaseConf.get("hbasesep.zookeeper.znode.parent", "/hbase");
    }

    private String getPort() {
        return hbaseConf.get("hbase.zookeeper.property.clientPort");
    }

    private void createZKPath(final String path, byte[] data, List<ACL> acl, CreateMode createMode)
            throws KeeperException, InterruptedException {
        createZKPath(path, data, acl, createMode, 5);
    }

    private void createZKPath(
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
        if (!isRunning) {
            // Protects from infinite waiting
            throw new RuntimeException("Consumer is not running");
        }
        return server.next();
    }

    public void close() {
        isRunning = false;
        try {
            zooKeeper.close();
            System.out.println("Closed connection to ZooKeeper");
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            rpcServer.stop();
            System.out.println("Closed HBase replication target server");
        }
    }

    private RecoverableZooKeeper connectZooKeeper() throws IOException {
        RecoverableZooKeeper zooKeeper =
                new RecoverableZooKeeper(
                        "localhost:" + getPort(),
                        20000,
                        event -> System.out.println("Watcher processed: " + event),
                        5,
                        200,
                        200,
                        null,
                        1);
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

    public void startReplication(String table, String columnFamily) {
        try (Connection connection = ConnectionFactory.createConnection(hbaseConf);
                Admin admin = connection.getAdmin(); ) {

            // clearExistingReplication(admin);
            ReplicationPeerConfig peerConfig = createPeerConfig(table, columnFamily);
            if (admin.listReplicationPeers().stream()
                    .map(ReplicationPeerDescription::getPeerId)
                    .anyMatch(replicationPeerId::equals)) {
                admin.updateReplicationPeerConfig(replicationPeerId, peerConfig);
            } else {
                admin.addReplicationPeer(replicationPeerId, peerConfig);
            }
            isRunning = true;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /** Instead of clearing existing replication under the same id, the config should be updated. */
    @Deprecated
    private void clearExistingReplication(Admin admin) throws IOException {
        admin.listReplicationPeers().stream()
                .filter(peer -> peer.getPeerId().equals(replicationPeerId))
                .forEach(
                        peer -> {
                            try {
                                admin.removeReplicationPeer(peer.getPeerId());
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        });
    }

    private ReplicationPeerConfig createPeerConfig(String table, String columnFamily) {
        HashMap tableMap = new HashMap<>();
        ArrayList<String> cFs = new ArrayList<>();
        cFs.add(columnFamily);
        tableMap.put(TableName.valueOf(table), cFs);
        return ReplicationPeerConfig.newBuilder()
                .setClusterKey("localhost:" + getPort() + ":" + getBaseString() + "/" + clusterKey)
                .setReplicateAllUserTables(false)
                .setTableCFsMap(tableMap)
                .build();
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
        rpcServer =
                RpcServerFactory.createRpcServer(
                        server,
                        name,
                        Arrays.asList(bsai),
                        initialIsa,
                        hbaseConf,
                        new FifoRpcScheduler(
                                hbaseConf,
                                hbaseConf.getInt("hbase.regionserver.handler.count", 10)));

        UUID uuid = UUID.nameUUIDFromBytes(Bytes.toBytes(clusterKey));
        createZKPath(
                getBaseString() + "/" + clusterKey,
                null,
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);
        createZKPath(
                getBaseString() + "/" + clusterKey + "/rs",
                null,
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);

        createZKPath(
                getBaseString() + "/" + clusterKey + "/hbaseid",
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
                getBaseString() + "/" + clusterKey + "/rs/" + serverName.getServerName(),
                null,
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL);

        return server;
    }
}

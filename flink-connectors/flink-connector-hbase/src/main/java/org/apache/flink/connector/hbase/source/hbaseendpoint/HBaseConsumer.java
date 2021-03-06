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

import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.connector.hbase.source.reader.HBaseEvent;
import org.apache.flink.connector.hbase.util.HBaseConfigurationUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.ipc.FifoRpcScheduler;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.ipc.RpcServerFactory;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.ReplicationPeerDescription;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.AdminService.BlockingInterface;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcController;
import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

/** Consumer of HBase WAL edits. */
public class HBaseConsumer implements ReplicationTargetInterface {

    // TODO
    String hostName = "localhost";

    private final String clusterKey;
    /** The id under which the replication target is made known to the source cluster. */
    private final String replicationPeerId;

    private final Configuration hbaseConf;
    private final RecoverableZooKeeper zooKeeper;
    private final RpcServer rpcServer;

    private boolean isRunning = false;

    private static final int QUEUE_CAPACITY = 1000;
    private final FutureCompletingBlockingQueue<HBaseEvent> walEdits;

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
        this.walEdits = new FutureCompletingBlockingQueue<>(QUEUE_CAPACITY);

        // Setup
        zooKeeper = connectToZooKeeper();
        rpcServer = createServer();
        registerAtZooKeeper();
    }

    private RecoverableZooKeeper connectToZooKeeper() throws IOException {
        RecoverableZooKeeper zooKeeper =
                new RecoverableZooKeeper(
                        "localhost:" + getZookeeperPort(),
                        20000,
                        event -> System.out.println("Watcher processed: " + event),
                        5,
                        200,
                        200,
                        null,
                        1);

        System.out.println("Connected to Zookeeper");

        return zooKeeper;
    }

    private RpcServer createServer() throws IOException {
        Server server = new EmptyHBaseServer(hbaseConf);
        InetSocketAddress initialIsa = new InetSocketAddress(hostName, 0);
        String name = "regionserver/" + initialIsa.toString();

        RpcServer.BlockingServiceAndInterface bsai =
                new RpcServer.BlockingServiceAndInterface(
                        AdminProtos.AdminService.newReflectiveBlockingService(this),
                        BlockingInterface.class);
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

        rpcServer.start();
        return rpcServer;
    }

    private void registerAtZooKeeper() throws KeeperException, InterruptedException {
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

        UUID uuid = UUID.nameUUIDFromBytes(Bytes.toBytes(clusterKey));
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
        zooKeeper.create(
                getBaseString() + "/" + clusterKey + "/rs/" + serverName.getServerName(),
                null,
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL);
    }

    public HBaseEvent next() {
        if (!isRunning) {
            // Protects from infinite waiting
            throw new RuntimeException("Consumer is not running");
        }

        try {
            return walEdits.take();
        } catch (InterruptedException e) {
            throw new RuntimeException(
                    "HBase replication endpoint couldn't retrieve element from queue", e);
        }
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

    public void startReplication(String table, List<String> columnFamilies) {
        if (isRunning) {
            throw new RuntimeException("HBase replication endpoint is already running");
        }
        try (Connection connection = ConnectionFactory.createConnection(hbaseConf);
                Admin admin = connection.getAdmin()) {

            // clearExistingReplication(admin);
            ReplicationPeerConfig peerConfig = createPeerConfig(table, columnFamilies);
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

    @Override
    public AdminProtos.ReplicateWALEntryResponse replicateWALEntry(
            RpcController controller, AdminProtos.ReplicateWALEntryRequest request)
            throws ServiceException {
        List<AdminProtos.WALEntry> entries = request.getEntryList();
        CellScanner cellScanner = ((HBaseRpcController) controller).cellScanner();

        for (final AdminProtos.WALEntry entry : entries) {
            final String table =
                    TableName.valueOf(entry.getKey().getTableName().toByteArray()).toString();
            final int count = entry.getAssociatedCellCount();

            for (int i = 0; i < count; i++) {
                try {
                    if (!cellScanner.advance()) {
                        throw new ArrayIndexOutOfBoundsException(
                                "Expected walentry to have "
                                        + count
                                        + "elements, but cell scanner did not have cell for index"
                                        + i);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }

                Cell cell = cellScanner.current();
                HBaseEvent event = HBaseEvent.fromCell(table, cell, i);

                try {
                    walEdits.put(0, event);
                } catch (InterruptedException exception) {
                    System.err.println("Error adding to Queue: " + exception);
                }
            }
        }

        return AdminProtos.ReplicateWALEntryResponse.newBuilder().build();
    }

    private ReplicationPeerConfig createPeerConfig(String table, List<String> columnFamilys) {
        HashMap tableMap = new HashMap<>();
        ArrayList<String> cFs = new ArrayList<>();
        cFs.addAll(columnFamilys);
        tableMap.put(TableName.valueOf(table), cFs);
        return ReplicationPeerConfig.newBuilder()
                .setClusterKey(
                        "localhost:"
                                + getZookeeperPort()
                                + ":"
                                + getBaseString()
                                + "/"
                                + clusterKey)
                .setReplicateAllUserTables(false)
                .setTableCFsMap(tableMap)
                .build();
    }

    private String getZookeeperPort() {
        return hbaseConf.get("hbase.zookeeper.property.clientPort");
    }

    private String getBaseString() {
        // TODO hbasesep.* will never be resolved
        return hbaseConf.get("hbasesep.zookeeper.znode.parent", "/hbase");
    }

    private void createZKPath(final String path, byte[] data, List<ACL> acl, CreateMode createMode)
            throws KeeperException, InterruptedException {
        try {
            if (zooKeeper.exists(path, false) == null) {
                zooKeeper.create(path, data, acl, createMode);
            }
        } catch (KeeperException e) {
            System.err.println("Error creating ZK path: " + e.getMessage());
            throw e;
        }
    }
}

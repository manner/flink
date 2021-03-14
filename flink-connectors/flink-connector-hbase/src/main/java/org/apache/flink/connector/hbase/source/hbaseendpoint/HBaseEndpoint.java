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
import org.apache.flink.connector.hbase.source.HBaseSourceOptions;
import org.apache.flink.connector.hbase.source.reader.HBaseSourceEvent;
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
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcController;
import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

/** Consumer of HBase WAL edits. */
public class HBaseEndpoint implements ReplicationTargetInterface {

    private static final Logger LOG = LoggerFactory.getLogger(HBaseEndpoint.class);
    private final String clusterKey;
    /** The id under which the replication target is made known to the source cluster. */
    private final String replicationPeerId;

    private final Configuration hbaseConf;
    private final RecoverableZooKeeper zooKeeper;
    private final RpcServer rpcServer;
    private final FutureCompletingBlockingQueue<HBaseSourceEvent> walEdits;
    private final String hostName;
    private final String tableName;
    private boolean isRunning = false;

    public HBaseEndpoint(byte[] serializedConfig, Properties properties)
            throws IOException, KeeperException, InterruptedException {
        this(HBaseConfigurationUtil.deserializeConfiguration(serializedConfig, null), properties);
    }

    public HBaseEndpoint(Configuration hbaseConf, Properties properties)
            throws InterruptedException, KeeperException, IOException {
        this(UUID.randomUUID().toString().substring(0, 5), hbaseConf, properties);
    }

    public HBaseEndpoint(String peerId, Configuration hbaseConf, Properties properties)
            throws IOException, KeeperException, InterruptedException {
        this.hbaseConf = hbaseConf;
        this.clusterKey = peerId + "_clusterKey";
        this.replicationPeerId = peerId;
        this.hostName = HBaseSourceOptions.getHostName(properties);
        this.tableName = HBaseSourceOptions.getTableName(properties);
        int queueCapacity = HBaseSourceOptions.getEndpointQueueCapacity(properties);
        this.walEdits = new FutureCompletingBlockingQueue<>(queueCapacity);

        // Setup
        zooKeeper = connectToZooKeeper();
        rpcServer = createServer();
        registerAtZooKeeper();
    }

    private RecoverableZooKeeper connectToZooKeeper() throws IOException {
        // Using private ZKUtil and RecoverableZooKeeper here, because the stability is so much
        // improved
        RecoverableZooKeeper zooKeeper =
                ZKUtil.connect(hbaseConf, getZookeeperClientAddress(), null);
        LOG.debug("Connected to Zookeeper");
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
        LOG.debug("Started rpc server at {}", initialIsa);
        return rpcServer;
    }

    private void registerAtZooKeeper() throws KeeperException, InterruptedException {
        createZKPath(getBaseString() + "/" + clusterKey, null, CreateMode.PERSISTENT);
        createZKPath(getBaseString() + "/" + clusterKey + "/rs", null, CreateMode.PERSISTENT);

        UUID uuid = UUID.nameUUIDFromBytes(Bytes.toBytes(clusterKey));
        createZKPath(
                getBaseString() + "/" + clusterKey + "/hbaseid",
                Bytes.toBytes(uuid.toString()),
                CreateMode.PERSISTENT);

        ServerName serverName =
                ServerName.valueOf(
                        hostName,
                        rpcServer.getListenerAddress().getPort(),
                        System.currentTimeMillis());
        createZKPath(
                getBaseString() + "/" + clusterKey + "/rs/" + serverName.getServerName(),
                null,
                CreateMode.EPHEMERAL);

        LOG.debug("Registered rpc server node at zookeeper");
    }

    /**
     * Blocks as long as the queue is empty. If the queue isn't empty it will try and get as many
     * elements as possible from the queue. It is not guaranteed that at least one element is in the
     * returned list.
     *
     * @return a list of {@link HBaseSourceEvent}.
     */
    public List<HBaseSourceEvent> getAll() {
        if (!isRunning) {
            // Protects from infinite waiting
            throw new RuntimeException("Consumer is not running");
        }
        List<HBaseSourceEvent> elements = new ArrayList<>();
        HBaseSourceEvent event;

        try {
            walEdits.getAvailabilityFuture().get();
            while ((event = walEdits.poll()) != null) {
                elements.add(event);
            }
            return elements;
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException("Can't retrieve elements from queue", e);
        }
    }

    public void wakeup() {
        walEdits.notifyAvailable();
    }

    public void close() throws InterruptedException {
        isRunning = false;
        try {
            zooKeeper.close();
            LOG.debug("Closed connection to ZooKeeper");
        } finally {
            rpcServer.stop();
            LOG.debug("Closed HBase replication target rpc server");
        }
    }

    public void startReplication(List<String> columnFamilies) throws IOException {
        if (isRunning) {
            throw new RuntimeException("HBase replication endpoint is already running");
        }
        try (Connection connection = ConnectionFactory.createConnection(hbaseConf);
                Admin admin = connection.getAdmin()) {

            ReplicationPeerConfig peerConfig = createPeerConfig(this.tableName, columnFamilies);
            if (admin.listReplicationPeers().stream()
                    .map(ReplicationPeerDescription::getPeerId)
                    .anyMatch(replicationPeerId::equals)) {
                admin.updateReplicationPeerConfig(replicationPeerId, peerConfig);
            } else {
                admin.addReplicationPeer(replicationPeerId, peerConfig);
            }
            isRunning = true;
        }
    }

    @Override
    public AdminProtos.ReplicateWALEntryResponse replicateWALEntry(
            RpcController controller, AdminProtos.ReplicateWALEntryRequest request)
            throws ServiceException {
        List<AdminProtos.WALEntry> entries = request.getEntryList();
        CellScanner cellScanner = ((HBaseRpcController) controller).cellScanner();

        try {
            for (final AdminProtos.WALEntry entry : entries) {
                final String table =
                        TableName.valueOf(entry.getKey().getTableName().toByteArray()).toString();
                final int count = entry.getAssociatedCellCount();
                for (int i = 0; i < count; i++) {
                    if (!cellScanner.advance()) {
                        throw new ArrayIndexOutOfBoundsException(
                                "Expected WAL entry to have "
                                        + count
                                        + "elements, but cell scanner did not have cell for index"
                                        + i);
                    }

                    Cell cell = cellScanner.current();
                    HBaseSourceEvent event = HBaseSourceEvent.fromCell(table, cell, i);
                    walEdits.put(0, event);
                }
            }
        } catch (Exception e) {
            throw new ServiceException("Could not replicate WAL entry in HBase endpoint", e);
        }

        return AdminProtos.ReplicateWALEntryResponse.newBuilder().build();
    }

    private ReplicationPeerConfig createPeerConfig(String table, List<String> columnFamilies) {
        Map<TableName, List<String>> tableCFsMap = new HashMap<>();
        tableCFsMap.put(TableName.valueOf(table), columnFamilies);
        return ReplicationPeerConfig.newBuilder()
                .setClusterKey(
                        getZookeeperClientAddress() + ":" + getBaseString() + "/" + clusterKey)
                .setReplicateAllUserTables(false)
                .setTableCFsMap(tableCFsMap)
                .build();
    }

    private String getZookeeperClientAddress() {
        return hbaseConf.get("hbase.zookeeper.quorum")
                + ":"
                + hbaseConf.get("hbase.zookeeper.property.clientPort");
    }

    private String getBaseString() {
        // TODO hbasesep.* will never be resolved
        return hbaseConf.get("hbasesep.zookeeper.znode.parent", "/hbase");
    }

    private void createZKPath(final String path, byte[] data, CreateMode createMode)
            throws InterruptedException {
        try {
            if (zooKeeper.exists(path, false) == null) {
                zooKeeper.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, createMode);
            }
        } catch (KeeperException e) {
            throw new RuntimeException("Error creating ZK path in Hbase replication endpoint", e);
        }
    }
}

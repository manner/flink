package org.apache.flink.connector.hbase.source.standalone;

import org.apache.flink.connector.hbase.source.standalone.util.zookeeper.ZkConnectException;
import org.apache.flink.connector.hbase.source.standalone.util.zookeeper.ZkUtil;
import org.apache.flink.connector.hbase.source.standalone.util.zookeeper.ZooKeeperItf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.ipc.FifoRpcScheduler;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hbase.thirdparty.com.google.common.collect.ArrayListMultimap;
import org.apache.hbase.thirdparty.com.google.common.collect.Maps;
import org.apache.hbase.thirdparty.com.google.common.collect.Multimap;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcController;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/** blah. */
public class Consumer extends AbstractRegionServer {
    private final String subscriptionId;
    private final ZooKeeperItf zk;
    private final Configuration hbaseConf;
    boolean running = false;
    private RpcServer rpcServer;
    private ServerName serverName;
    private ZKWatcher zkWatcher;
    private String zkNodePath;

    public Consumer(
            String subscriptionId, String hostName, Configuration hbaseConf, ZooKeeperItf zk)
            throws IOException {
        this.subscriptionId = subscriptionId;
        this.zk = zk;
        this.hbaseConf = hbaseConf;

        InetSocketAddress initialIsa = new InetSocketAddress(hostName, 9811);
        if (initialIsa.getAddress() == null) {
            throw new IllegalArgumentException("Failed resolve of " + initialIsa);
        }
        String name = "regionserver/" + initialIsa.toString();
        this.rpcServer =
                org.apache.hadoop.hbase.ipc.RpcServerFactory.createRpcServer(
                        this,
                        name,
                        getServices(),
                        initialIsa,
                        hbaseConf,
                        new FifoRpcScheduler(
                                hbaseConf,
                                hbaseConf.getInt("hbase.regionserver.handler.count", 10)));

        this.serverName =
                ServerName.valueOf(
                        hostName,
                        rpcServer.getListenerAddress().getPort(),
                        System.currentTimeMillis());
        this.zkWatcher = new ZKWatcher(hbaseConf, this.serverName.toString(), null);
    }

    public static void main(String[] args)
            throws IOException, ZkConnectException, KeeperException, InterruptedException {
        UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser("tempusername"));
        Configuration conf = null;
        try {
            conf = parseConfig();
        } catch (SAXException | IOException | ParserConfigurationException e) {
            e.printStackTrace();
        }
        Admin admin = ConnectionFactory.createConnection(conf).getAdmin();

        String subscriptionId = "test";

        if (admin.listReplicationPeers().stream()
                .anyMatch(descriptor -> descriptor.getPeerId().equals(subscriptionId))) {
            admin.removeReplicationPeer(subscriptionId);
        }

        String port = conf.get("hbase.zookeeper.property.clientPort");
        ZooKeeperItf zk = ZkUtil.connect("localhost:" + port, 20000000);

        String baseZkPath = "/hbase";
        String basePath = baseZkPath + "/" + subscriptionId;
        UUID uuid =
                UUID.nameUUIDFromBytes(
                        Bytes.toBytes(
                                subscriptionId)); // always gives the same uuid for the same name
        ZkUtil.createPath(zk, basePath + "/hbaseid", Bytes.toBytes(uuid.toString()));
        ZkUtil.createPath(zk, basePath + "/rs");

        Consumer consumer = new Consumer(subscriptionId, "localhost", conf, zk);
        consumer.start();

        ReplicationPeerConfig peerConfig =
                ReplicationPeerConfig.newBuilder()
                        .setClusterKey("localhost:" + port + ":" + basePath)
                        .build();
        admin.addReplicationPeer(subscriptionId, peerConfig);

        admin.listReplicationPeers()
                .forEach(
                        replicationPeerDescription ->
                                System.out.println(replicationPeerDescription.toString()));

        while (true) {
            Thread.sleep(Long.MAX_VALUE);
        }
    }

    public static Configuration parseConfig()
            throws SAXException, IOException, ParserConfigurationException {
        Configuration hbaseConf = HBaseConfiguration.create();

        if (new File(TestClusterStarter.CONFIG_PATH).exists()) {
            Document config =
                    DocumentBuilderFactory.newInstance()
                            .newDocumentBuilder()
                            .parse(TestClusterStarter.CONFIG_PATH);
            NodeList nodes = config.getDocumentElement().getElementsByTagName("property");
            for (int i = 0; i < nodes.getLength(); i++) {
                Element e = (Element) nodes.item(i);
                hbaseConf.set(
                        e.getElementsByTagName("name").item(0).getTextContent(),
                        e.getElementsByTagName("value").item(0).getTextContent());
            }
        } else {
            System.err.println("[Warning] Could not find test hbase config at " + hbaseConf);
        }

        return hbaseConf;
    }

    @Override
    public AdminProtos.ReplicateWALEntryResponse replicateWALEntry(
            final RpcController controller, final AdminProtos.ReplicateWALEntryRequest request) {

        System.out.println("WAL!!!!");
        List<AdminProtos.WALEntry> entries = request.getEntryList();
        CellScanner cells = ((HBaseRpcController) controller).cellScanner();

        for (final AdminProtos.WALEntry entry : entries) {
            TableName tableName =
                    (entry.getKey().getWriteTime() < 0)
                            ? null
                            : TableName.valueOf(entry.getKey().getTableName().toByteArray());
            Multimap<ByteBuffer, Cell> keyValuesPerRowKey = ArrayListMultimap.create();
            final Map<ByteBuffer, byte[]> payloadPerRowKey = Maps.newHashMap();
            int count = entry.getAssociatedCellCount();
            for (int i = 0; i < count; i++) {
                try {
                    if (!cells.advance()) {
                        throw new ArrayIndexOutOfBoundsException(
                                "Expected=" + count + ", index=" + i);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }

                // this signals to us that we simply need to skip over count of cells
                if (tableName == null) {
                    continue;
                }

                Cell cell = cells.current();
                KeyValue kv = KeyValueUtil.ensureKeyValue(cell);
                System.out.println(new String(kv.getValueArray()));
            }
        }
        return AdminProtos.ReplicateWALEntryResponse.newBuilder().build();
    }

    public void start() throws IOException, InterruptedException, KeeperException {

        rpcServer.start();

        // Publish our existence in ZooKeeper
        zkNodePath =
                hbaseConf.get("hbasesep.zookeeper.znode.parent", "/hbase")
                        + "/"
                        + subscriptionId
                        + "/rs/"
                        + serverName.getServerName();
        zk.create(zkNodePath, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        this.running = true;
    }

    private List<RpcServer.BlockingServiceAndInterface> getServices() {
        List<RpcServer.BlockingServiceAndInterface> bssi = new ArrayList<>(1);
        bssi.add(
                new RpcServer.BlockingServiceAndInterface(
                        AdminProtos.AdminService.newReflectiveBlockingService(this),
                        AdminProtos.AdminService.BlockingInterface.class));
        return bssi;
    }
}

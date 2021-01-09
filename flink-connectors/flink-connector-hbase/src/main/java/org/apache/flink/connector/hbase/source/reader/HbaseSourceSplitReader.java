package org.apache.flink.connector.hbase.source.reader;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.connector.hbase.source.split.HbaseSourceSplit;
import org.apache.flink.connector.hbase.source.standalone.AbstractRegionServer;
import org.apache.flink.connector.hbase.source.standalone.util.zookeeper.ZkUtil;
import org.apache.flink.connector.hbase.source.standalone.util.zookeeper.ZooKeeperItf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
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
import org.apache.hbase.thirdparty.com.google.common.collect.ArrayListMultimap;
import org.apache.hbase.thirdparty.com.google.common.collect.Maps;
import org.apache.hbase.thirdparty.com.google.common.collect.Multimap;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcController;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;

import javax.annotation.Nullable;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * A {@link SplitReader} implementation for Hbase.
 */
public class HbaseSourceSplitReader extends AbstractRegionServer implements SplitReader<Tuple3<byte[], Long, Long>, HbaseSourceSplit> {
	private final String subscriptionId;
	private final Configuration hbaseConf;
	boolean running = false;
	private ZooKeeperItf zk;
	private RpcServer rpcServer;
	private ServerName serverName;
	private ZKWatcher zkWatcher;
	private String zkNodePath;

	public HbaseSourceSplitReader() {
		System.out.println("constructing Split Reader");
		String hostName = "localhost";
		subscriptionId = "test";
		hbaseConf = new Configuration();
		String port = hbaseConf.get("hbase.zookeeper.property.clientPort");
		zk = null;
		try {

			zk = ZkUtil.connect("localhost:" + port, 20000000);

			Admin admin = ConnectionFactory.createConnection(hbaseConf).getAdmin();
			if (admin
				.listReplicationPeers()
				.stream()
				.anyMatch(descriptor -> descriptor.getPeerId().equals(subscriptionId))) {
				admin.removeReplicationPeer(subscriptionId);
			}

			String baseZkPath = "/hbase";
			String basePath = baseZkPath + "/" + subscriptionId;
			UUID uuid = UUID.nameUUIDFromBytes(Bytes.toBytes(subscriptionId)); // always gives the same uuid for the same name
			ZkUtil.createPath(zk, basePath + "/hbaseid", Bytes.toBytes(uuid.toString()));
			ZkUtil.createPath(zk, basePath + "/rs");

			InetSocketAddress initialIsa = new InetSocketAddress(hostName, 9811);
			if (initialIsa.getAddress() == null) {
				throw new IllegalArgumentException("Failed resolve of " + initialIsa);
			}
			String name = "regionserver/" + initialIsa.toString();
			this.rpcServer = org.apache.hadoop.hbase.ipc.RpcServerFactory.createRpcServer(
				this,
				name,
				getServices(),
				initialIsa,
				hbaseConf,
				new FifoRpcScheduler(
					hbaseConf,
					hbaseConf.getInt("hbase.regionserver.handler.count", 10)));

			this.serverName = ServerName.valueOf(
				hostName,
				rpcServer.getListenerAddress().getPort(),
				System.currentTimeMillis());
			this.zkWatcher = new ZKWatcher(hbaseConf, this.serverName.toString(), null);

			rpcServer.start();

			// Publish our existence in ZooKeeper
			zkNodePath = hbaseConf.get("hbasesep.zookeeper.znode.parent", "/hbase")
				+ "/" + subscriptionId + "/rs/" + serverName.getServerName();
			zk.create(zkNodePath, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
			this.running = true;

			ReplicationPeerConfig peerConfig = ReplicationPeerConfig.newBuilder()
				.setClusterKey("localhost:" + port + ":" + basePath)
				.build();
			admin.addReplicationPeer(subscriptionId, peerConfig);

			admin
				.listReplicationPeers()
				.forEach(replicationPeerDescription -> System.out.println(replicationPeerDescription
					.toString()));
			while (true) {
				Thread.sleep(Long.MAX_VALUE);
			}
		} catch (Exception e) {

		}
	}

	@Override
	public AdminProtos.ReplicateWALEntryResponse replicateWALEntry(
		final RpcController controller,
		final AdminProtos.ReplicateWALEntryRequest request) {
		System.out.println("WAL!!!!");
		List<AdminProtos.WALEntry> entries = request.getEntryList();
		CellScanner cells = ((HBaseRpcController) controller).cellScanner();

		for (final AdminProtos.WALEntry entry : entries) {
			TableName tableName = (entry.getKey().getWriteTime() < 0) ? null :
				TableName.valueOf(entry.getKey().getTableName().toByteArray());
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

	@Override
	public RecordsWithSplitIds<Tuple3<byte[], Long, Long>> fetch() throws IOException {
		System.out.println("fetching in Split Reader");
		return null;
	}

	@Override
	public void handleSplitsChanges(SplitsChange<HbaseSourceSplit> splitsChanges) {

	}

	@Override
	public void wakeUp() {

	}

	@Override
	public void close() throws Exception {

	}

	private List<RpcServer.BlockingServiceAndInterface> getServices() {
		List<RpcServer.BlockingServiceAndInterface> bssi = new ArrayList<>(1);
		bssi.add(new RpcServer.BlockingServiceAndInterface(
			AdminProtos.AdminService.newReflectiveBlockingService(this),
			AdminProtos.AdminService.BlockingInterface.class));
		return bssi;
	}

	private static class HbaseSplitRecords<T> implements RecordsWithSplitIds<T> {
		private final Collection<T> records;

		private HbaseSplitRecords(Collection<T> records) {
			this.records = records;
		}

		@Nullable
		@Override
		public String nextSplit() {
			return "a";
		}

		@Nullable
		@Override
		public T nextRecordFromSplit() {
			if (!records.iterator().hasNext()) {
				return null;
			} else {
				return records.iterator().next();
			}
		}

		@Override
		public Set<String> finishedSplits() {
			return null;
		}
	}
}

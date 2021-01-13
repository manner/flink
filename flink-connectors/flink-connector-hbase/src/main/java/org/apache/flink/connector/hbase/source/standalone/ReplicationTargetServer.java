package org.apache.flink.connector.hbase.source.standalone;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.hadoop.hbase.ipc.PriorityFunction;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.RequestHeader;
import org.apache.hbase.thirdparty.com.google.protobuf.Message;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcController;
import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;
import org.eclipse.jetty.util.ArrayQueue;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;

/** Bla. */
public class ReplicationTargetServer extends AbstractRegionServer implements PriorityFunction {

    private final Queue<byte[]> walEdits;

    public ReplicationTargetServer() {
        walEdits = new ArrayQueue<>();
    }

    public byte[] next() {
        return walEdits.poll();
    }

    @Override
    public AdminProtos.ReplicateWALEntryResponse replicateWALEntry(
            RpcController controller, AdminProtos.ReplicateWALEntryRequest request)
            throws ServiceException {
        System.err.println("Replication!!!");

        List<AdminProtos.WALEntry> entries = request.getEntryList();
        CellScanner cells = ((HBaseRpcController) controller).cellScanner();

        for (final AdminProtos.WALEntry entry : entries) {
            TableName tableName =
                    (entry.getKey().getWriteTime() < 0)
                            ? null
                            : TableName.valueOf(entry.getKey().getTableName().toByteArray());
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
                System.out.println("consumes: " + Arrays.toString(kv.getValueArray()));
                walEdits.add(kv.getValueArray());
            }
        }
        return AdminProtos.ReplicateWALEntryResponse.newBuilder().build();
    }

    @Override
    public int getPriority(RequestHeader header, Message param, User user) {
        return org.apache.hadoop.hbase.HConstants.NORMAL_QOS;
    }

    @Override
    public long getDeadline(RequestHeader header, Message param) {
        return 0;
    }
}

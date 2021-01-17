package org.apache.flink.connector.hbase.source.standalone;

import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;

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

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/** Bla. */
public class ReplicationTargetServer extends AbstractRegionServer implements PriorityFunction {

    private final FutureCompletingBlockingQueue<byte[]> walEdits;

    public ReplicationTargetServer() {
        walEdits = new FutureCompletingBlockingQueue<>();
    }

    public byte[] next() {
        try {
            return walEdits.take();
        } catch (InterruptedException e) {
            System.err.println("Couldn't retrieve element from queue: " + e);
        }
        return null;
    }

    @Override
    public AdminProtos.ReplicateWALEntryResponse replicateWALEntry(
            RpcController controller, AdminProtos.ReplicateWALEntryRequest request)
            throws ServiceException {
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
                try {
                    walEdits.put(0, kv.getValueArray());
                } catch (InterruptedException exception) {
                    System.err.println("Error adding to Queue: " + exception);
                }
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

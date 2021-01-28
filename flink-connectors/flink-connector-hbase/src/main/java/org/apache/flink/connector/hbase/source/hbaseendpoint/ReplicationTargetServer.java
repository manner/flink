package org.apache.flink.connector.hbase.source.hbaseendpoint;

import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.connector.hbase.source.reader.HBaseEvent;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.hadoop.hbase.ipc.PriorityFunction;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.RequestHeader;
import org.apache.hbase.thirdparty.com.google.common.collect.ArrayListMultimap;
import org.apache.hbase.thirdparty.com.google.common.collect.Multimap;
import org.apache.hbase.thirdparty.com.google.protobuf.Message;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcController;
import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/** Bla. */
public class ReplicationTargetServer extends AbstractRegionServer implements PriorityFunction {

    private static final int QUEUE_CAPACITY = 10;
    private final FutureCompletingBlockingQueue<HBaseEvent> walEdits;

    public ReplicationTargetServer() {
        walEdits = new FutureCompletingBlockingQueue<>(QUEUE_CAPACITY);
    }

    public HBaseEvent next() {
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
            Multimap<ByteBuffer, Cell> keyValuesPerRowKey = ArrayListMultimap.create();

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
                ByteBuffer rowKey =
                        ByteBuffer.wrap(
                                cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());

                keyValuesPerRowKey.put(rowKey, kv);
            }
            for (final ByteBuffer rowKeyBuffer : keyValuesPerRowKey.keySet()) {
                final List<Cell> keyValues = (List<Cell>) keyValuesPerRowKey.get(rowKeyBuffer);

                final String table = tableName.toString();
                final String row = new String(CellUtil.cloneRow(keyValues.get(0)));
                final long timestamp = keyValues.get(0).getTimestamp();

                for (Cell cell : keyValues) {
                    final String cf = new String(CellUtil.cloneFamily(cell));
                    final String qualifier = new String(CellUtil.cloneQualifier(cell));
                    final byte[] payload = CellUtil.cloneValue(cell);
                    final int offset = cell.getRowOffset(); // which offset is the right one?
                    final Cell.Type type = cell.getType();
                    HBaseEvent event =
                            new HBaseEvent(
                                    type, row, table, cf, qualifier, payload, timestamp, offset);

                    try {
                        walEdits.put(0, event);
                    } catch (InterruptedException exception) {
                        System.err.println("Error adding to Queue: " + exception);
                    }
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

package org.apache.flink.connector.hbase.sink.writer;

import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.connector.hbase.sink.HBaseSinkCommittable;
import org.apache.flink.connector.hbase.sink.HBaseSinkSerializer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/** HBaseWriter. */
public class HBaseWriter<IN> implements SinkWriter<IN, HBaseSinkCommittable, HBaseWriterState> {

    private final Configuration hbaseConfiguration;
    private final HBaseSinkSerializer<IN> sinkSerializer;
    private final String tableName;

    public HBaseWriter(
            Sink.InitContext context,
            String tableName,
            HBaseSinkSerializer<IN> sinkSerializer,
            Configuration hbaseConfiguration) {
        this.tableName = tableName;
        this.sinkSerializer = sinkSerializer;
        this.hbaseConfiguration = hbaseConfiguration;
    }

    @Override
    public void write(IN element, Context context) throws IOException {
        Put put = new Put(sinkSerializer.serializeRowKey(element));
        put.addColumn(
                sinkSerializer.serializeColumnFamily(element),
                sinkSerializer.serializeQualifier(element),
                sinkSerializer.serializePayload(element));
        try (Connection connection = ConnectionFactory.createConnection(hbaseConfiguration)) {
            Table table = connection.getTable(TableName.valueOf(tableName));
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public List<HBaseSinkCommittable> prepareCommit(boolean flush) throws IOException {
        return Collections.emptyList();
    }

    @Override
    public List<HBaseWriterState> snapshotState() throws IOException {
        return Collections.emptyList();
    }

    @Override
    public void close() throws Exception {}
}

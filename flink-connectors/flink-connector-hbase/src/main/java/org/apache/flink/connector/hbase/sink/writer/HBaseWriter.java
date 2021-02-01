package org.apache.flink.connector.hbase.sink.writer;

import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.connector.hbase.sink.HBaseSinkCommittable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/** HBaseWriter. */
public class HBaseWriter<IN> implements SinkWriter<IN, HBaseSinkCommittable, HBaseWriterState> {

    private final Configuration hbaseConfiguration;
    private final String table;
    private final String columnFamily;
    private final String qualifier;

    public HBaseWriter(
            Sink.InitContext context,
            String table,
            String columnFamily,
            String qualifier,
            Configuration hbaseConfiguration) {
        this.table = table;
        this.columnFamily = columnFamily;
        this.qualifier = qualifier;

        this.hbaseConfiguration = hbaseConfiguration;
    }

    @Override
    public void write(IN element, Context context) throws IOException {
        Put put = new Put(Bytes.toBytes(element.toString()));
        put.addColumn(
                Bytes.toBytes(columnFamily),
                Bytes.toBytes(qualifier),
                Bytes.toBytes(element.toString()));
        try (Connection connection = ConnectionFactory.createConnection(hbaseConfiguration)) {
            Table table = connection.getTable(TableName.valueOf(this.table));
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

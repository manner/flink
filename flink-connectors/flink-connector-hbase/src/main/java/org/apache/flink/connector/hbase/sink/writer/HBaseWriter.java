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
    private final TableName tableName;

    public HBaseWriter(
            Sink.InitContext context, TableName tableName, Configuration hbaseConfiguration) {
        this.hbaseConfiguration = hbaseConfiguration;
        this.tableName = tableName;
    }

    @Override
    public void write(IN element, Context context) throws IOException {
        Put put = new Put(Bytes.toBytes(element.toString()));
        put.addColumn(
                Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes(element.toString()));
        try (Connection connection =
                ConnectionFactory.createConnection(this.hbaseConfiguration); ) {
            Table table = connection.getTable(tableName);
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

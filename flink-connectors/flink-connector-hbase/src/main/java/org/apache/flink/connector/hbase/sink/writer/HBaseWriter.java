package org.apache.flink.connector.hbase.sink.writer;

import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.connector.hbase.sink.HBaseSinkCommittable;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** HBaseWriter. */
public class HBaseWriter<IN> implements SinkWriter<IN, HBaseSinkCommittable, HBaseWriterState> {

    private List<HBaseSinkCommittable> elements;

    public HBaseWriter(Sink.InitContext context) {
        elements = new ArrayList<>();
    }

    @Override
    public void write(IN element, Context context) throws IOException {
        Put put = new Put(Bytes.toBytes("row"));
        put.addColumn(
                Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes(element.toString()));
        elements.add(new HBaseSinkCommittable(put));
    }

    @Override
    public List<HBaseSinkCommittable> prepareCommit(boolean flush) throws IOException {
        List<HBaseSinkCommittable> result = elements;
        elements = new ArrayList<>();
        return result;
    }

    @Override
    public List<HBaseWriterState> snapshotState() throws IOException {
        return Collections.emptyList();
    }

    @Override
    public void close() throws Exception {}
}

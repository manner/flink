package org.apache.flink.connector.hbase.sink.writer;

import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.connector.hbase.sink.HBaseSinkCommittable;

import java.io.IOException;
import java.util.List;

/** HBaseWriter. */
public class HBaseWriter<IN> implements SinkWriter<IN, HBaseSinkCommittable, HBaseWriterState> {
    @Override
    public void write(IN element, Context context) throws IOException {}

    @Override
    public List<HBaseSinkCommittable> prepareCommit(boolean flush) throws IOException {
        return null;
    }

    @Override
    public List<HBaseWriterState> snapshotState() throws IOException {
        return null;
    }

    @Override
    public void close() throws Exception {}
}

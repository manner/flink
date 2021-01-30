package org.apache.flink.connector.hbase.sink;

import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.connector.hbase.sink.writer.HBaseWriterState;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

/** HBaseSink. */
public class HBaseSink<IN> implements Sink<IN, HBaseSinkCommittable, HBaseWriterState, Void> {

    @Override
    public SinkWriter<IN, HBaseSinkCommittable, HBaseWriterState> createWriter(
            InitContext context, List<HBaseWriterState> states) throws IOException {
        return null;
    }

    @Override
    public Optional<Committer<HBaseSinkCommittable>> createCommitter() throws IOException {
        return Optional.empty();
    }

    @Override
    public Optional<GlobalCommitter<HBaseSinkCommittable, Void>> createGlobalCommitter()
            throws IOException {
        return Optional.empty();
    }

    @Override
    public Optional<SimpleVersionedSerializer<HBaseSinkCommittable>> getCommittableSerializer() {
        return Optional.empty();
    }

    @Override
    public Optional<SimpleVersionedSerializer<Void>> getGlobalCommittableSerializer() {
        return Optional.empty();
    }

    @Override
    public Optional<SimpleVersionedSerializer<HBaseWriterState>> getWriterStateSerializer() {
        return Optional.empty();
    }
}

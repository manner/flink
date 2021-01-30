package org.apache.flink.connector.hbase.sink.committer;

import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.connector.hbase.sink.HBaseSinkCommittable;

import java.io.IOException;
import java.util.List;

/** HBaseCommitter. */
public class HBaseCommitter implements Committer<HBaseSinkCommittable> {
    @Override
    public List<HBaseSinkCommittable> commit(List<HBaseSinkCommittable> committables)
            throws IOException {
        return null;
    }

    @Override
    public void close() throws Exception {}
}

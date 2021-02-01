package org.apache.flink.connector.hbase.sink.committer;

import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.connector.hbase.sink.HBaseSinkCommittable;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/** HBaseCommitter. */
public class HBaseCommitter implements Committer<HBaseSinkCommittable> {
    private org.apache.hadoop.conf.Configuration hbaseConfiguration;
    private TableName tableName;

    public HBaseCommitter(
            TableName tableName, org.apache.hadoop.conf.Configuration hbaseConfiguration) {
        this.tableName = tableName;
        this.hbaseConfiguration = hbaseConfiguration;
    }

    @Override
    public List<HBaseSinkCommittable> commit(List<HBaseSinkCommittable> committables)
            throws IOException {
        try (Connection connection =
                ConnectionFactory.createConnection(this.hbaseConfiguration); ) {
            Table table = connection.getTable(tableName);
            for (HBaseSinkCommittable committable : committables) {
                RowMutations mutations = RowMutations.of(committable.getMutations());
                table.mutateRow(mutations);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return Collections.emptyList();
    }

    @Override
    public void close() throws Exception {}
}

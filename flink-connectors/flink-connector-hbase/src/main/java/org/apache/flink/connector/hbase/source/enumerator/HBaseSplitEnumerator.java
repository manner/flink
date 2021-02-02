package org.apache.flink.connector.hbase.source.enumerator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.hbase.source.split.HBaseSourceSplit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Queue;

/** The enumerator class for Hbase source. */
@Internal
public class HBaseSplitEnumerator
        implements SplitEnumerator<HBaseSourceSplit, Collection<HBaseSourceSplit>> {
    private final SplitEnumeratorContext<HBaseSourceSplit> context;
    private final Queue<HBaseSourceSplit> remainingSplits;
    private final String table;
    private final Configuration hbaseConfiguration;

    public HBaseSplitEnumerator(
            SplitEnumeratorContext<HBaseSourceSplit> context,
            Configuration hbaseConfiguration,
            String table) {
        this.context = context;
        this.remainingSplits = new ArrayDeque<>();
        this.table = table;
        this.hbaseConfiguration = hbaseConfiguration;
    }

    @Override
    public void start() {
        try (Connection connection = ConnectionFactory.createConnection(this.hbaseConfiguration);
                Admin admin = connection.getAdmin()) {
            ColumnFamilyDescriptor[] colFamDes =
                    admin.getDescriptor(TableName.valueOf(this.table)).getColumnFamilies();
            List<HBaseSourceSplit> splits = new ArrayList<>();
            for (ColumnFamilyDescriptor colFamDe : colFamDes) {
                splits.add(
                        new HBaseSourceSplit(
                                String.format("1234%s", new String(colFamDe.getName())),
                                "localhost",
                                table,
                                new String(colFamDe.getName()),
                                hbaseConfiguration));
            }

            addSplits(splits);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() {}

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {

        final HBaseSourceSplit nextSplit = remainingSplits.poll();
        if (nextSplit != null) {
            context.assignSplit(nextSplit, subtaskId);
        } else {
            context.signalNoMoreSplits(subtaskId);
        }
    }

    @Override
    public void addSplitsBack(List<HBaseSourceSplit> splits, int subtaskId) {
        remainingSplits.addAll(splits);
    }

    @Override
    public void addReader(int subtaskId) {
        System.out.println("addReader");
        HBaseSourceSplit nextSplit = remainingSplits.poll();
        if (nextSplit != null) {
            context.assignSplit(nextSplit, subtaskId);
        } else {
            context.signalNoMoreSplits(subtaskId);
        }
    }

    @Override
    public Collection<HBaseSourceSplit> snapshotState() throws Exception {
        return remainingSplits;
    }

    public void addSplits(Collection<HBaseSourceSplit> splits) {
        remainingSplits.addAll(splits);
    }
}

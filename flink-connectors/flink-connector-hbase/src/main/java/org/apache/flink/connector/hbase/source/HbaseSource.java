package org.apache.flink.connector.hbase.source;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.hbase.source.enumerator.HbaseSplitEnumerator;
import org.apache.flink.connector.hbase.source.reader.HbaseSourceReader;
import org.apache.flink.connector.hbase.source.split.HbaseSourceSplit;
import org.apache.flink.connector.hbase.source.split.HbaseSourceSplitSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/** A connector for Hbase. */
public class HbaseSource implements Source<byte[], HbaseSourceSplit, Collection<HbaseSourceSplit>> {

    public static org.apache.hadoop.conf.Configuration tempHbaseConfig; // TODO remove asap

    private final Boundedness boundedness;

    private final String tableName;
    private final transient org.apache.hadoop.conf.Configuration
            hbaseConfiguration; // TODO find out why source needs to be serializable

    public HbaseSource(
            Boundedness boundedness,
            String tableName,
            org.apache.hadoop.conf.Configuration hbaseConfiguration) {
        this.boundedness = boundedness;
        this.tableName = tableName;
        this.hbaseConfiguration = hbaseConfiguration;

        tempHbaseConfig = hbaseConfiguration;
    }

    @Override
    public Boundedness getBoundedness() {
        return boundedness;
    }

    @Override
    public SourceReader<byte[], HbaseSourceSplit> createReader(SourceReaderContext readerContext)
            throws Exception {
        System.out.println("createReader");
        return new HbaseSourceReader(new Configuration(), readerContext);
    }

    @Override
    public SplitEnumerator<HbaseSourceSplit, Collection<HbaseSourceSplit>> restoreEnumerator(
            SplitEnumeratorContext<HbaseSourceSplit> enumContext,
            Collection<HbaseSourceSplit> checkpoint)
            throws Exception {
        System.out.println("restoreEnumerator");

        HbaseSplitEnumerator enumerator = new HbaseSplitEnumerator(enumContext);
        enumerator.addSplits(checkpoint);
        return enumerator;
    }

    @Override
    public SplitEnumerator<HbaseSourceSplit, Collection<HbaseSourceSplit>> createEnumerator(
            SplitEnumeratorContext<HbaseSourceSplit> enumContext) throws Exception {
        System.out.println("createEnumerator");
        List<HbaseSourceSplit> splits = new ArrayList<>();

        List<String> regionIds = Arrays.asList("region1"); // , "region2");
        regionIds.forEach(
                regionId -> {
                    splits.add(
                            new HbaseSourceSplit(
                                    String.format("1234%s", regionId),
                                    "localhost",
                                    tableName,
                                    regionId,
                                    hbaseConfiguration));
                });
        HbaseSplitEnumerator enumerator = new HbaseSplitEnumerator(enumContext);
        enumerator.addSplits(splits);
        return enumerator;
    }

    @Override
    public SimpleVersionedSerializer<HbaseSourceSplit> getSplitSerializer() {
        System.out.println("getSplitSerializer");

        return new HbaseSourceSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<Collection<HbaseSourceSplit>>
            getEnumeratorCheckpointSerializer() {
        System.out.println("getEnumeratorCheckpointSerializer");
        return null;
    }
}

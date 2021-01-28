package org.apache.flink.connector.hbase.source;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.hbase.source.enumerator.HBaseSplitEnumerator;
import org.apache.flink.connector.hbase.source.reader.HBaseSourceReader;
import org.apache.flink.connector.hbase.source.split.HBaseSourceSplit;
import org.apache.flink.connector.hbase.source.split.HBaseSourceSplitSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/** A connector for Hbase. */
public class HBaseSource<T> implements Source<T, HBaseSourceSplit, Collection<HBaseSourceSplit>> {

    public static org.apache.hadoop.conf.Configuration tempHbaseConfig; // TODO remove asap
    private final String tableName;

    private final Boundedness boundedness;

    private final DeserializationSchema<T> deserializationSchema;
    private final transient org.apache.hadoop.conf.Configuration
            hbaseConfiguration; // TODO find out why source needs to be serializable

    public HBaseSource(
            Boundedness boundedness,
            DeserializationSchema<T> deserializationSchema,
            String table,
            org.apache.hadoop.conf.Configuration hbaseConfiguration) {
        this.boundedness = boundedness;
        this.hbaseConfiguration = hbaseConfiguration;
        this.deserializationSchema = deserializationSchema;
        this.tableName = table;

        tempHbaseConfig = hbaseConfiguration;
    }

    @Override
    public Boundedness getBoundedness() {
        return boundedness;
    }

    @Override
    public SourceReader<T, HBaseSourceSplit> createReader(SourceReaderContext readerContext)
            throws Exception {
        System.out.println("createReader");
        return new HBaseSourceReader<>(new Configuration(), deserializationSchema, readerContext);
    }

    @Override
    public SplitEnumerator<HBaseSourceSplit, Collection<HBaseSourceSplit>> restoreEnumerator(
            SplitEnumeratorContext<HBaseSourceSplit> enumContext,
            Collection<HBaseSourceSplit> checkpoint)
            throws Exception {
        System.out.println("restoreEnumerator");

        HBaseSplitEnumerator enumerator = new HBaseSplitEnumerator(enumContext, tempHbaseConfig, tableName);
        enumerator.addSplits(checkpoint);
        return enumerator;
    }

    @Override
    public SplitEnumerator<HBaseSourceSplit, Collection<HBaseSourceSplit>> createEnumerator(
            SplitEnumeratorContext<HBaseSourceSplit> enumContext) throws Exception {
        System.out.println("createEnumerator");
        return new HBaseSplitEnumerator(enumContext, tempHbaseConfig, tableName);
    }

    @Override
    public SimpleVersionedSerializer<HBaseSourceSplit> getSplitSerializer() {
        System.out.println("getSplitSerializer");

        return new HBaseSourceSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<Collection<HBaseSourceSplit>>
            getEnumeratorCheckpointSerializer() {
        System.out.println("getEnumeratorCheckpointSerializer");
        return null;
    }
}

package org.apache.flink.connector.hbase.source.reader;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.hbase.source.split.HBaseSourceSplit;
import org.apache.flink.connector.hbase.source.split.HBaseSourceSplitState;

import java.util.Map;

/** The source reader for Hbase. */
public class HBaseSourceReader<T>
        extends SingleThreadMultiplexSourceReaderBase<
                HBaseEvent, T, HBaseSourceSplit, HBaseSourceSplitState> {
    public HBaseSourceReader(
            Configuration config,
            DeserializationSchema<T> deserializationSchema,
            SourceReaderContext context) {
        super(
                HBaseSourceSplitReader::new,
                new HBaseRecordEmitter<T>(deserializationSchema),
                config,
                context);
        System.out.println("constructing in Source Reader");
    }

    @Override
    protected void onSplitFinished(Map<String, HBaseSourceSplitState> finishedSplitIds) {
        context.sendSplitRequest();
    }

    @Override
    protected HBaseSourceSplitState initializedState(HBaseSourceSplit split) {
        System.out.println("initializedState");
        return new HBaseSourceSplitState(split);
    }

    @Override
    protected HBaseSourceSplit toSplitType(String splitId, HBaseSourceSplitState splitState) {
        return splitState.toSplit();
    }
}

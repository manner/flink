/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.hbase.source;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.hbase.source.enumerator.HBaseSourceEnumeratorCheckpointSerializer;
import org.apache.flink.connector.hbase.source.enumerator.HBaseSplitEnumerator;
import org.apache.flink.connector.hbase.source.reader.HBaseSourceDeserializer;
import org.apache.flink.connector.hbase.source.reader.HBaseSourceReader;
import org.apache.flink.connector.hbase.source.split.HBaseSourceSplit;
import org.apache.flink.connector.hbase.source.split.HBaseSourceSplitSerializer;
import org.apache.flink.connector.hbase.util.HBaseConfigurationUtil;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.util.Collection;

/** A connector for Hbase. */
public class HBaseSource<T> implements Source<T, HBaseSourceSplit, Collection<HBaseSourceSplit>> {

    private final String tableName;

    private final Boundedness boundedness;

    private final HBaseSourceDeserializer<T> sourceDeserializer;
    private final byte[] serializedConfig;

    public HBaseSource(
            Boundedness boundedness,
            HBaseSourceDeserializer<T> sourceDeserializer,
            String table,
            org.apache.hadoop.conf.Configuration hbaseConfiguration) {
        this.boundedness = boundedness;
        this.serializedConfig = HBaseConfigurationUtil.serializeConfiguration(hbaseConfiguration);
        this.sourceDeserializer = sourceDeserializer;
        this.tableName = table;
    }

    @Override
    public Boundedness getBoundedness() {
        return boundedness;
    }

    @Override
    public SourceReader<T, HBaseSourceSplit> createReader(SourceReaderContext readerContext)
            throws Exception {
        System.out.println("createReader");
        return new HBaseSourceReader<>(serializedConfig, sourceDeserializer, readerContext);
    }

    @Override
    public SplitEnumerator<HBaseSourceSplit, Collection<HBaseSourceSplit>> restoreEnumerator(
            SplitEnumeratorContext<HBaseSourceSplit> enumContext,
            Collection<HBaseSourceSplit> checkpoint)
            throws Exception {
        System.out.println("restoreEnumerator");

        HBaseSplitEnumerator enumerator =
                new HBaseSplitEnumerator(enumContext, serializedConfig, tableName);
        enumerator.addSplits(checkpoint);
        return enumerator;
    }

    @Override
    public SplitEnumerator<HBaseSourceSplit, Collection<HBaseSourceSplit>> createEnumerator(
            SplitEnumeratorContext<HBaseSourceSplit> enumContext) throws Exception {
        System.out.println("createEnumerator");
        return new HBaseSplitEnumerator(enumContext, serializedConfig, tableName);
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
        return new HBaseSourceEnumeratorCheckpointSerializer();
    }
}

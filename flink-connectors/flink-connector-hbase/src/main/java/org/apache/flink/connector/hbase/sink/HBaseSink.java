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

package org.apache.flink.connector.hbase.sink;

import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.connector.hbase.sink.writer.HBaseWriter;
import org.apache.flink.connector.hbase.sink.writer.HBaseWriterState;
import org.apache.flink.connector.hbase.util.HBaseConfigurationUtil;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

/** HBaseSink. */
public class HBaseSink<IN> implements Sink<IN, HBaseSinkCommittable, HBaseWriterState, Void> {

    private final byte[] serializedConfig;
    private final String tableName;
    private final HBaseSinkSerializer<IN> sinkSerializer;

    public HBaseSink(
            String tableName,
            HBaseSinkSerializer<IN> sinkSerializer,
            Configuration hbaseConfiguration) {
        this.tableName = tableName;
        this.sinkSerializer = sinkSerializer;

        this.serializedConfig = HBaseConfigurationUtil.serializeConfiguration(hbaseConfiguration);
    }

    @Override
    public SinkWriter<IN, HBaseSinkCommittable, HBaseWriterState> createWriter(
            InitContext context, List<HBaseWriterState> states) throws IOException {
        return new HBaseWriter<>(context, tableName, sinkSerializer, serializedConfig);
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

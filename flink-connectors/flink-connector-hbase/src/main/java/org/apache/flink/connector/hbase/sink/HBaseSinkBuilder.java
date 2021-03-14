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

import org.apache.hadoop.conf.Configuration;

import java.util.Properties;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The builder class to create an {@link HBaseSink}. The following example shows how to create an
 * HBaseSink that writes Long values to HBase.
 *
 * <pre>{@code
 * HBaseSink<Long> hbaseSink =
 *      HBaseSink.<Long>builder()
 *          .setTableName(tableName)
 *          .setSinkSerializer(new HBaseLongSerializer())
 *          .setHBaseConfiguration(hbaseConfig)
 *          .build();
 * }</pre>
 *
 * <p>Here is an example for the Serializer:
 *
 * <pre>{@code
 * static class HBaseLongSerializer implements HBaseSinkSerializer<Long> {
 *     @Override
 *     public HBaseEvent serialize(Long event) {
 *         return HBaseEvent.putWith(                  // or deleteWith()
 *                 event.toString(),                   // rowId
 *                 "exampleColumnFamily",              // column family
 *                 "exampleQualifier",                 // qualifier
 *                 Bytes.toBytes(event.toString()));   // payload
 *     }
 * }
 * }</pre>
 *
 * <p>A SerializationSchema is always required, as well as a table name to write to and an
 * HBaseConfiguration.
 *
 * <p>By default each HBaseWriter has a queue limit of 1000 entries used for batching. This can be
 * changed with {@link #setQueueLimit(int)}. The maximum allowed latency of an event can be set by
 * {@link #setMaxLatencyMs(int)}. After the specified elements will be sent to HBase no matter how
 * many elements are currently in the batching queue.
 */
public class HBaseSinkBuilder<IN> {

    private static final String[] REQUIRED_CONFIGS = {HBaseSinkOptions.TABLE_NAME.key()};
    private final Properties properties;
    private Configuration hbaseConfiguration;
    private HBaseSinkSerializer<IN> sinkSerializer;

    protected HBaseSinkBuilder() {
        this.sinkSerializer = null;
        this.hbaseConfiguration = null;
        this.properties = new Properties();
    }

    public HBaseSinkBuilder<IN> setTableName(String tableName) {
        return setProperty(HBaseSinkOptions.TABLE_NAME.key(), tableName);
    }

    public HBaseSinkBuilder<IN> setSinkSerializer(HBaseSinkSerializer<IN> sinkSerializer) {
        this.sinkSerializer = sinkSerializer;
        return this;
    }

    public HBaseSinkBuilder<IN> setHBaseConfiguration(Configuration hbaseConfiguration) {
        this.hbaseConfiguration = hbaseConfiguration;
        return this;
    }

    public HBaseSinkBuilder<IN> setQueueLimit(int queueLimit) {
        return setProperty(HBaseSinkOptions.QUEUE_LIMIT.key(), String.valueOf(queueLimit));
    }

    public HBaseSinkBuilder<IN> setMaxLatencyMs(int maxLatencyMs) {
        return setProperty(HBaseSinkOptions.MAX_LATENCY.key(), String.valueOf(maxLatencyMs));
    }

    public HBaseSinkBuilder<IN> setProperty(final String key, final String value) {
        this.properties.setProperty(key, value);
        return this;
    }

    public HBaseSinkBuilder<IN> setProperties(final Properties properties) {
        this.properties.putAll(properties);
        return this;
    }

    public HBaseSink<IN> build() {
        sanityCheck();
        return new HBaseSink<>(sinkSerializer, hbaseConfiguration, properties);
    }

    private void sanityCheck() {
        for (String requiredConfig : REQUIRED_CONFIGS) {
            checkNotNull(
                    properties.getProperty(requiredConfig),
                    String.format("Property %s is required but not provided", requiredConfig));
        }

        checkNotNull(sinkSerializer, "No sink serializer was specified.");
        checkNotNull(hbaseConfiguration, "No hbase configuration was specified.");
    }
}

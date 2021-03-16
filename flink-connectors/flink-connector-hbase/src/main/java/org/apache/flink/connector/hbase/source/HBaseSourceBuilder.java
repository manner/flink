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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.hbase.source.reader.HBaseSourceDeserializer;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The builder class to create an {@link HBaseSource}.
 *
 * <p>The following example shows a minimum setup to create a HBase Source that reads String values
 * from each cell.
 *
 * <pre>{@code
 * HBaseSource<String> source = HBaseSource
 *      .<String>builder()
 *      .setSourceDeserializer(new CustomHBaseDeserializationSchema())
 *      .setTableName("test-table")
 *      .setHBaseConfiguration(new HBaseTestClusterUtil().getConfig())
 *      .build();
 *
 * public static class CustomHBaseDeserializationSchema implements HBaseSourceDeserializer<String> {
 *
 *         @Override
 *         public String deserialize(HBaseEvent event) {
 *             return new String(event.getPayload());
 *         }
 *     }
 * }</pre>
 *
 * <p>A DeserializationSchema is always required to be set, as well as a TableName to read from and
 * a HBaseConfiguration.
 *
 * <p>By default each HBaseEndpoint has a queue capacity of 1000 entries for WALedits, this can be
 * changed with {@link #setQueueCapacity(int queueSize)}. The hostname of the created RPC Servers
 * can be changed with {@link #setHostName(String hostname)}.
 */
public class HBaseSourceBuilder<IN> {

    private static final ConfigOption<?>[] REQUIRED_CONFIGS = {HBaseSourceOptions.TABLE_NAME};
    private final Configuration sourceConfiguration;
    private org.apache.hadoop.conf.Configuration hbaseConfiguration;
    private HBaseSourceDeserializer<IN> sourceDeserializer;

    protected HBaseSourceBuilder() {
        this.sourceDeserializer = null;
        this.hbaseConfiguration = null;
        this.sourceConfiguration = new Configuration();
    }

    /**
     * Sets the table name from which changes will be processed.
     *
     * @param tableName the HBase table name.
     * @return this HBaseSourceBuilder.
     */
    public HBaseSourceBuilder<IN> setTableName(String tableName) {
        sourceConfiguration.setString(HBaseSourceOptions.TABLE_NAME, tableName);
        return this;
    }

    /**
     * Sets the Deserializer, that will be used to deserialize cell entries in HBase.
     *
     * @param sourceDeserializer the HBase Source Deserializer.
     * @return this HBaseSourceBuilder.
     */
    public HBaseSourceBuilder<IN> setSourceDeserializer(
            HBaseSourceDeserializer<IN> sourceDeserializer) {
        this.sourceDeserializer = sourceDeserializer;
        return this;
    }

    /**
     * Sets the HBaseConfiguration.
     *
     * @param hbaseConfiguration the HBaseConfiguration.
     * @return this HBaseSourceBuilder.
     */
    public HBaseSourceBuilder<IN> setHBaseConfiguration(
            org.apache.hadoop.conf.Configuration hbaseConfiguration) {
        this.hbaseConfiguration = hbaseConfiguration;
        return this;
    }

    /**
     * Sets the queue capacity for incoming WALedits in the HBaseEndpoint.
     *
     * @param queueCapacity integer value of the queue capacity.
     * @return this KafkaSourceBuilder.
     */
    public HBaseSourceBuilder<IN> setQueueCapacity(int queueCapacity) {
        sourceConfiguration.setInteger(HBaseSourceOptions.ENDPOINT_QUEUE_CAPACITY, queueCapacity);
        return this;
    }

    /**
     * Sets the hostname of the RPC Server in the HBaseEndpoint.
     *
     * @param hostName the hostname String.
     * @return this KafkaSourceBuilder.
     */
    public HBaseSourceBuilder<IN> setHostName(String hostName) {
        sourceConfiguration.setString(HBaseSourceOptions.HOST_NAME, hostName);
        return this;
    }

    public HBaseSource<IN> build() {
        sanityCheck();
        return new HBaseSource<>(sourceDeserializer, hbaseConfiguration, sourceConfiguration);
    }

    private void sanityCheck() {
        for (ConfigOption<?> requiredConfig : REQUIRED_CONFIGS) {
            checkNotNull(
                    sourceConfiguration.get(requiredConfig),
                    String.format("Config option %s is required but not provided", requiredConfig));
        }

        checkNotNull(sourceDeserializer, "No source deserializer was specified.");
        checkNotNull(hbaseConfiguration, "No hbase configuration was specified.");
    }
}

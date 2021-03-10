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

import org.apache.flink.connector.hbase.source.reader.HBaseSourceDeserializer;

import org.apache.hadoop.conf.Configuration;

import java.util.Properties;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** The builder class to create an {@link HBaseSource}. */
public class HBaseSourceBuilder<IN> {

    private static final String[] REQUIRED_CONFIGS = {HBaseSourceOptions.TABLE_NAME.key()};
    private final Properties properties;
    private Configuration hbaseConfiguration;
    private HBaseSourceDeserializer<IN> sourceDeserializer;

    protected HBaseSourceBuilder() {
        this.sourceDeserializer = null;
        this.hbaseConfiguration = null;
        this.properties = new Properties();
    }

    public HBaseSourceBuilder<IN> setTableName(String tableName) {
        return setProperty(HBaseSourceOptions.TABLE_NAME.key(), tableName);
    }

    public HBaseSourceBuilder<IN> setSourceDeserializer(
            HBaseSourceDeserializer<IN> sourceDeserializer) {
        this.sourceDeserializer = sourceDeserializer;
        return this;
    }

    public HBaseSourceBuilder<IN> setHBaseConfiguration(Configuration hbaseConfiguration) {
        this.hbaseConfiguration = hbaseConfiguration;
        return this;
    }

    public HBaseSourceBuilder<IN> setQueueCapacity(int queueCapacity) {
        return setProperty(
                HBaseSourceOptions.ENDPOINT_QUEUE_CAPACITY.key(), String.valueOf(queueCapacity));
    }

    public HBaseSourceBuilder<IN> setHostName(String hostName) {
        return setProperty(HBaseSourceOptions.HOST_NAME.key(), hostName);
    }

    public HBaseSourceBuilder<IN> setProperty(final String key, final String value) {
        this.properties.setProperty(key, value);
        return this;
    }

    public HBaseSourceBuilder<IN> setProperties(final Properties properties) {
        this.properties.putAll(properties);
        return this;
    }

    public HBaseSource<IN> build() {
        sanityCheck();
        return new HBaseSource<>(sourceDeserializer, hbaseConfiguration, properties);
    }

    private void sanityCheck() {
        for (String requiredConfig : REQUIRED_CONFIGS) {
            checkNotNull(
                    properties.getProperty(requiredConfig),
                    String.format("Property %s is required but not provided", requiredConfig));
        }

        checkNotNull(sourceDeserializer, "No source deserializer was specified.");
        checkNotNull(hbaseConfiguration, "No hbase configuration was specified.");
    }
}

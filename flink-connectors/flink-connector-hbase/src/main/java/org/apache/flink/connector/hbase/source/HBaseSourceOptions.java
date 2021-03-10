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
import org.apache.flink.configuration.ConfigOptions;

import java.util.Properties;
import java.util.function.Function;

/** Configuration options for {@link HBaseSource}. */
public class HBaseSourceOptions {

    public static final ConfigOption<String> TABLE_NAME =
            ConfigOptions.key("table_name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The table from which changes will be received");

    public static final ConfigOption<Integer> ENDPOINT_QUEUE_CAPACITY =
            ConfigOptions.key("endpoint.queue.capacity")
                    .intType()
                    .defaultValue(1000)
                    .withDescription("The maximum queue size in the HBase endpoint");

    public static final ConfigOption<String> HOST_NAME =
            ConfigOptions.key("endpoint.hostname")
                    .stringType()
                    .defaultValue("localhost")
                    .withDescription(
                            "The hostname of the RPC server in the HBaseEndpoint receiving events from HBase");

    @SuppressWarnings("unchecked")
    public static <T> T getOption(
            Properties props, ConfigOption configOption, Function<String, T> parser) {
        String value = props.getProperty(configOption.key());
        return (T) (value == null ? configOption.defaultValue() : parser.apply(value));
    }

    public static String getTableName(Properties properties) {
        return getOption(properties, TABLE_NAME, Function.identity());
    }

    public static int getEndpointQueueCapacity(Properties properties) {
        return getOption(properties, ENDPOINT_QUEUE_CAPACITY, Integer::parseInt);
    }

    public static String getHostName(Properties properties) {
        return getOption(properties, HOST_NAME, Function.identity());
    }
}

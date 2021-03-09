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

package org.apache.flink.connector.hbase.source.playground;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.hbase.testutil.FailureSink;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterJobClient;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.Iterator;
import java.util.List;

/** Playground. */
public class CheckpointAndCancelShowcase {

    private static final Logger LOG = LoggerFactory.getLogger(CheckpointAndCancelShowcase.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(2000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.setParallelism(1);
        DataStream<String> stream = env.fromCollection(new Numbers(), String.class);

        FailureSink<String> failureSink =
                new FailureSink<String>(2500, TypeInformation.of(String.class)) {
                    @Override
                    public void collectValue(String value) throws Exception {
                        List<String> checkpointed = getCheckpointedValues();
                        LOG.info(unCheckpointedValues + " " + checkpointed);
                        if (checkpointed.contains(value)) {
                            LOG.error(("That was not exactly once!"));
                        }
                    }
                };
        stream.addSink(failureSink);
        MiniClusterJobClient jobClient = (MiniClusterJobClient) env.executeAsync();
        MiniCluster miniCluster = miniCluster(jobClient);
        LOG.info("Started execution ...");
        while (!miniCluster.isRunning()) {
            Thread.sleep(100);
        }
        LOG.info("Flinkcluster is running");
        Thread.sleep(5000);
        miniCluster.close();
        while (miniCluster.isRunning()) {
            Thread.sleep(100);
        }
        LOG.info("Terminated ...");
    }

    private static MiniCluster miniCluster(MiniClusterJobClient jobClient)
            throws IllegalAccessException, NoSuchFieldException {
        Field field = MiniClusterJobClient.class.getDeclaredField("miniCluster");
        field.setAccessible(true);
        return (MiniCluster) field.get(jobClient);
    }

    private static class Numbers implements Iterator<String>, Serializable {

        private int i = 0;

        {
            LOG.info("Constructed iterator");
        }

        @Override
        public boolean hasNext() {
            return true;
        }

        @Override
        public String next() {
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            LOG.info("Next called with i=" + i);
            return "" + (i++);
        }
    }
}

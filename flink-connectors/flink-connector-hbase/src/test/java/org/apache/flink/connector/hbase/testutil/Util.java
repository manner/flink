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

package org.apache.flink.connector.hbase.testutil;

import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterJobClient;

import java.lang.reflect.Field;

/** Provides utility for testing with flink miniclusters. */
public class Util {

    public static MiniCluster miniCluster(MiniClusterJobClient jobClient)
            throws IllegalAccessException, NoSuchFieldException {
        Field field = MiniClusterJobClient.class.getDeclaredField("miniCluster");
        field.setAccessible(true);
        return (MiniCluster) field.get(jobClient);
    }

    public static void waitForClusterStart(MiniCluster miniCluster, boolean verbose)
            throws InterruptedException {
        if (verbose) {
            System.out.println("Started flink minicluster execution ...");
        }

        while (!miniCluster.isRunning()) {
            Thread.sleep(100);
        }

        if (verbose) {
            System.out.println("Flink minicluster is running");
        }
    }
}

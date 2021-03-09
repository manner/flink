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

import org.apache.flink.connector.hbase.testutil.HBaseTestClusterUtil;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/** This class is only used to start a (reusable) test cluster from the test root. */
@Deprecated
public class ClusterStartTest {

    private static final Logger LOG = LoggerFactory.getLogger(ClusterStartTest.class);

    @BeforeClass
    public static void setup() throws IOException {
        new HBaseTestClusterUtil().startCluster();
    }

    @AfterClass
    public static void teardown() {}

    public void startCluster() throws Exception {
        LOG.info("Pass");
        while (true) {
            Thread.sleep(30000);
        }
    }
}

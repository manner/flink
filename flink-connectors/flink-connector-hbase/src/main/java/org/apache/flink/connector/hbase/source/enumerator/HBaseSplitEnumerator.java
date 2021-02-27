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

package org.apache.flink.connector.hbase.source.enumerator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.hbase.source.split.HBaseSourceSplit;
import org.apache.flink.connector.hbase.util.HBaseConfigurationUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Queue;

/** The enumerator class for Hbase source. */
@Internal
public class HBaseSplitEnumerator
        implements SplitEnumerator<HBaseSourceSplit, Collection<HBaseSourceSplit>> {
    private final SplitEnumeratorContext<HBaseSourceSplit> context;
    private final Queue<HBaseSourceSplit> remainingSplits;
    private final String table;
    private final byte[] serializedConfig;

    public HBaseSplitEnumerator(
            SplitEnumeratorContext<HBaseSourceSplit> context,
            byte[] serializedConfig,
            String table) {
        this.context = context;
        this.remainingSplits = new ArrayDeque<>();
        this.table = table;

        this.serializedConfig = serializedConfig;
        System.out.println("Constructed HBase Split enumerator");
    }

    @Override
    public void start() {
        Configuration hbaseConfiguration =
                HBaseConfigurationUtil.deserializeConfiguration(this.serializedConfig, null);
        try (Connection connection = ConnectionFactory.createConnection(hbaseConfiguration);
                Admin admin = connection.getAdmin()) {
            ColumnFamilyDescriptor[] colFamDes =
                    admin.getDescriptor(TableName.valueOf(this.table)).getColumnFamilies();
            List<HBaseSourceSplit> splits = new ArrayList<>();
            for (ColumnFamilyDescriptor colFamDe : colFamDes) {
                splits.add(
                        new HBaseSourceSplit(
                                // TODO find better id pattern
                                String.format("1234%s", new String(colFamDe.getName())),
                                "localhost",
                                table,
                                new String(colFamDe.getName())));
            }

            addSplits(splits);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() {}

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {

        final HBaseSourceSplit nextSplit = remainingSplits.poll();
        if (nextSplit != null) {
            context.assignSplit(nextSplit, subtaskId);
        } else {
            context.signalNoMoreSplits(subtaskId);
        }
    }

    @Override
    public void addSplitsBack(List<HBaseSourceSplit> splits, int subtaskId) {
        remainingSplits.addAll(splits);
    }

    @Override
    public void addReader(int subtaskId) {
        System.out.println("addReader");
        HBaseSourceSplit nextSplit = remainingSplits.poll();
        if (nextSplit != null) {
            context.assignSplit(nextSplit, subtaskId);
        } else {
            context.signalNoMoreSplits(subtaskId);
        }
    }

    @Override
    public Collection<HBaseSourceSplit> snapshotState() throws Exception {
        return remainingSplits;
    }

    public void addSplits(Collection<HBaseSourceSplit> splits) {
        remainingSplits.addAll(splits);
    }
}

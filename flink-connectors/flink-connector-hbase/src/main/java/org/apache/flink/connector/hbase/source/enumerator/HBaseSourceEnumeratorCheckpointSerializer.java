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

import org.apache.flink.connector.hbase.source.split.HBaseSourceSplit;
import org.apache.flink.connector.hbase.source.split.HBaseSourceSplitSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/** Checkpoint serializer for hBase source. (De-)Serializes the collection of splits */
public class HBaseSourceEnumeratorCheckpointSerializer
        implements SimpleVersionedSerializer<Collection<HBaseSourceSplit>> {

    private HBaseSourceSplitSerializer splitSerializer = new HBaseSourceSplitSerializer();

    private static final Logger LOG =
            LoggerFactory.getLogger(HBaseSourceEnumeratorCheckpointSerializer.class);

    @Override
    public int getVersion() {
        return 0;
    }

    @Override
    public byte[] serialize(Collection<HBaseSourceSplit> checkpointState) throws IOException {
        LOG.debug("serialize");
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(baos)) {
            out.writeInt(checkpointState.size());
            for (HBaseSourceSplit split : checkpointState) {
                byte[] serializedSplit = splitSerializer.serialize(split);
                out.write(serializedSplit.length);
                out.write(serializedSplit);
            }
            out.flush();
            return baos.toByteArray();
        }
    }

    @Override
    public Collection<HBaseSourceSplit> deserialize(int version, byte[] serialized)
            throws IOException {
        LOG.debug("deserialize");
        List<HBaseSourceSplit> checkPointState = new ArrayList<>();
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                DataInputStream in = new DataInputStream(bais)) {
            int numSplits = in.readInt();
            for (int i = 0; i < numSplits; i++) {
                int splitSize = in.readInt();
                byte[] serializedSplit = new byte[splitSize];
                in.read(serializedSplit);
                HBaseSourceSplit split =
                        splitSerializer.deserialize(splitSerializer.getVersion(), serializedSplit);
                checkPointState.add(split);
            }
        }
        return checkPointState;
    }
}

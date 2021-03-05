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

package org.apache.flink.connector.hbase.source.reader;

import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.hbase.source.split.HBaseSourceSplitState;

/** The {@link RecordEmitter} implementation for {@link HBaseSourceReader}. */
public class HBaseRecordEmitter<T> implements RecordEmitter<HBaseEvent, T, HBaseSourceSplitState> {

    private final HBaseSourceDeserializer<T> sourceDeserializer;

    public HBaseRecordEmitter(HBaseSourceDeserializer<T> sourceDeserializer) {
        this.sourceDeserializer = sourceDeserializer;
    }

    @Override
    public void emitRecord(
            HBaseEvent event, SourceOutput<T> output, HBaseSourceSplitState splitState)
            throws Exception {
        System.out.println("EVENT: " + event);
        if (!splitState.isAlreadyProcessedEvent(event)) {
            splitState.notifyEmittedEvent(event);
            T deserializedPayload = sourceDeserializer.deserialize(event);
            output.collect(deserializedPayload, event.getTimestamp());
        } else {
            // Ignore event, was already processed
        }
    }
}

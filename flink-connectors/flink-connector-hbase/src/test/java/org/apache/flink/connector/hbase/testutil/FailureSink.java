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

import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

/** TODO documentation. */
public abstract class FailureSink<T> extends RichSinkFunction<T>
        implements CheckpointedFunction, CheckpointListener {

    private final boolean verbose;
    private final long activateAfter;
    private final TypeInformation<T> typeInfo;
    private boolean active = false;
    private boolean hasSeenAtLeastOneCheckpoint = false;
    private boolean hasSeenAtLeastOneInput = false;

    protected final List<T> unCheckpointedValues = new ArrayList<>();
    protected transient ListState<T> checkpointedValues;

    public FailureSink(boolean verbose, long activateAfter, TypeInformation<T> typeInfo) {
        this.verbose = verbose;
        this.activateAfter = activateAfter;
        this.typeInfo = typeInfo;
    }

    @Override
    public void invoke(T value, Context context) throws Exception {
        if (verbose) {
            System.out.println(value + " " + active);
        }
        unCheckpointedValues.add(value);
        collectValue(value);
        hasSeenAtLeastOneInput = true;
        throwFailureIfActive();
    }

    public void collectValue(T value) throws Exception {}

    public void checkpoint() throws Exception {}

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        System.out.println(
                "FailureSink.notifyCheckpointComplete has been called with checkpointId="
                        + checkpointId);
        hasSeenAtLeastOneCheckpoint = true;
        throwFailureIfActive();
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        if (verbose) {
            System.out.println("FailureSink.snapshotState has been called");
        }
        checkpointedValues.addAll(unCheckpointedValues);
        unCheckpointedValues.clear();
        checkpoint();
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        if (verbose) {
            System.out.println("FailureSink.initializeState has been called");
        }

        hasSeenAtLeastOneCheckpoint = false;
        hasSeenAtLeastOneInput = false;

        ListStateDescriptor<T> descriptor = new ListStateDescriptor<>("checkpointed", typeInfo);

        checkpointedValues = context.getOperatorStateStore().getListState(descriptor);

        new Timer().schedule(activation(), activateAfter);
    }

    public List<T> getCheckpointedValues() {
        try {
            List<T> checkpointed = new ArrayList<>();
            checkpointedValues.get().forEach(checkpointed::add);
            return checkpointed;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    private TimerTask activation() {
        return new TimerTask() {
            @Override
            public void run() {
                if (hasSeenAtLeastOneCheckpoint && hasSeenAtLeastOneInput) {
                    active = true;
                    if (verbose) {
                        System.out.println("FailureSink activated");
                    }
                } else {
                    new Timer().schedule(activation(), activateAfter / 2);
                }
            }
        };
    }

    private void throwFailureIfActive() {
        if (active) {
            if (verbose) {
                System.out.println("FailureSink triggered");
            }
            throw new RuntimeException("Failure Sink throws error");
        }
    }
}

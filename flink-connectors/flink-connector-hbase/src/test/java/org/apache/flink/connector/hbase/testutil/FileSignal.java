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

import org.apache.flink.util.FileUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Utility class to signal events of tests such as success or failure. Allows to get signals out of
 * flink without using web sockets or success exceptions or similar.
 */
public class FileSignal {

    private static final Logger LOG = LoggerFactory.getLogger(FileSignal.class);

    private static final long POLL_INTERVAL = 100; // ms
    private static final File SIGNAL_FOLDER = new File("signal");

    private static final String SUCCESS_SIGNAL = "success";
    private static final String FAILURE_SIGNAL = "failure";

    public static void signal(String signalName) {
        File signalFile = signalFile(signalName);
        try {
            signalFile.createNewFile();
            LOG.debug("Created signal file at " + signalFile.getAbsolutePath());
        } catch (IOException e) {
            throw new RuntimeException("Could not create signal file " + signalName, e);
        }
    }

    public static void signalSuccess() {
        signal(SUCCESS_SIGNAL);
    }

    public static void signalFailure() {
        signal(FAILURE_SIGNAL);
    }

    public static CompletableFuture<String> awaitSignal(String signalName) {
        File signalFile = signalFile(signalName);
        return CompletableFuture.supplyAsync(
                () -> {
                    while (!signalFile.exists()) {
                        try {
                            Thread.sleep(POLL_INTERVAL);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(
                                    "Waiting for signal " + signalName + "was interrupted.", e);
                        }
                    }
                    cleanupSignal(signalName);
                    return signalName;
                });
    }

    public static void awaitSignalThrowOnFailure(String signalName, long timeout, TimeUnit timeUnit)
            throws InterruptedException, ExecutionException, TimeoutException {
        String result =
                (String)
                        CompletableFuture.anyOf(
                                        awaitSignal(signalName), awaitSignal(FAILURE_SIGNAL))
                                .get(timeout, timeUnit);
        if (result.equals(FAILURE_SIGNAL)) {
            throw new RuntimeException("Waiting for signal " + signalName + " yielded failure");
        }
    }

    public static void awaitSuccess(long timeout, TimeUnit timeUnit)
            throws InterruptedException, ExecutionException, TimeoutException {
        awaitSignalThrowOnFailure(SUCCESS_SIGNAL, timeout, timeUnit);
    }

    public static void makeFolder() {
        SIGNAL_FOLDER.mkdirs();
    }

    public static void cleanupSignal(String signalName) {
        File signalFile = signalFile(signalName);
        signalFile.delete();
    }

    public static void cleanupFolder() throws IOException {
        FileUtils.deleteDirectory(SIGNAL_FOLDER);
    }

    private static File signalFile(String signalName) {
        return SIGNAL_FOLDER.toPath().resolve(signalName + ".signal").toFile();
    }
}

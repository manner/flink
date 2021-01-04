/*
 * Copyright 2012 NGDATA nv
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.hbase.source.standalone.util.concurrent;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


/**
 * This file was copied from Mule: Copyright (c) MuleSource, Inc. All rights reserved. http://www.mulesource.com
 * A handler for unexecutable tasks that waits until the task can be submitted for
 * execution or times out. Generously snipped from the jsr166 repository at: <a
 * HREF="http://gee.cs.oswego.edu/cgi-bin/viewcvs.cgi/jsr166/src/main/java/util/concurrent/ThreadPoolExecutor.java"></a>.
 */
public class WaitPolicy implements RejectedExecutionHandler {
	private final long time;
	private final TimeUnit timeUnit;

	/**
	 * Constructs a <tt>WaitPolicy</tt> which waits (almost) forever.
	 */
	public WaitPolicy() {
		// effectively waits forever
		this(Long.MAX_VALUE, TimeUnit.SECONDS);
	}

	/**
	 * Constructs a <tt>WaitPolicy</tt> with timeout. A negative <code>time</code>
	 * value is interpreted as <code>Long.MAX_VALUE</code>.
	 */
	public WaitPolicy(long time, TimeUnit timeUnit) {
		super();
		this.time = (time < 0 ? Long.MAX_VALUE : time);
		this.timeUnit = timeUnit;
	}

	@Override
	public void rejectedExecution(Runnable r, ThreadPoolExecutor e) {
		try {
			if (e.isShutdown() || !e.getQueue().offer(r, this.time, this.timeUnit)) {
				throw new RejectedExecutionException();
			}
		} catch (InterruptedException ie) {
			Thread.currentThread().interrupt();
			throw new RejectedExecutionException(ie);
		}
	}

}

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

package org.apache.flink.table.runtime.functions.scalar;

import org.apache.flink.annotation.Internal;

/**
 * Prepares a value from the first argument of a {@link SharedArgumentScalarFunction}, e.g. reads a
 * document. Functions whose preparers have the same class share the prepared value, so define one
 * preparer class for each kind of value.
 *
 * <p>The generated code creates one instance for each shared input with {@link
 * SharedArgumentScalarFunction#createPreparer()}, so a preparer can keep resources, e.g. a parser.
 * Only one thread uses an instance, and {@link #close()} is called when the operator closes.
 *
 * <p>The preparation must be deterministic and must not fail. Errors are part of the prepared
 * value, so that each function can handle them.
 *
 * @param <I> the internal data of the argument
 * @param <S> the prepared value
 */
@Internal
public interface ArgumentPreparer<I, S> {

    /** Prepares the value from a non-{@code NULL} argument. */
    S prepare(I argument);

    /** Releases the resources of the preparer. */
    default void close() throws Exception {}
}

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
import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.functions.SpecializedFunction;

/**
 * A built-in scalar function whose first argument is prepared once per record and shared with other
 * calls, e.g. a document that several calls read.
 *
 * <p>The function creates its {@link ArgumentPreparer} in {@link #createPreparer()}. Calls whose
 * preparers have the same class and get the same input share the prepared value, even if they are
 * different functions. Instead of {@code eval}, the function implements {@code evalShared} methods,
 * which take the prepared value as their first parameter, followed by the other arguments of the
 * call. A {@code NULL} input is passed as {@code null}. The planner checks the call against the
 * {@code evalShared} methods.
 *
 * <p>The first call that runs prepares the value, so a call in a branch that isn't taken costs
 * nothing.
 *
 * <p>Each prepared value class has exactly one preparer class, so that all functions that prepare
 * it share it. Each value class needs a test {@code <Value>SharingTest} that extends {@code
 * SharedArgumentFunctionTestBase} and only lists the calls and two inputs. It fails if the calls
 * don't share, e.g. because of a second preparer class. {@code SharedArgumentFunctionsCoverageTest}
 * fails if the test is missing.
 *
 * @param <I> the internal data of the argument
 * @param <S> the prepared value
 */
@Internal
public abstract class SharedArgumentScalarFunction<I, S> extends BuiltInScalarFunction {

    protected SharedArgumentScalarFunction(
            BuiltInFunctionDefinition definition, SpecializedFunction.SpecializedContext context) {
        super(definition, context);
    }

    /**
     * Creates the preparer of the value that {@code evalShared} gets as its first parameter. The
     * generated code creates one for each input, with the function of the first call that shares
     * it.
     *
     * <p>It must always return an instance of the same class, since the planner calls it to get the
     * class. The planner calls it before {@code open()}, so it must not depend on it.
     */
    public abstract ArgumentPreparer<I, S> createPreparer();

    /** Returns the class of the preparer, which decides with which calls the value is shared. */
    public final Class<?> getPreparerClass() {
        return createPreparer().getClass();
    }
}

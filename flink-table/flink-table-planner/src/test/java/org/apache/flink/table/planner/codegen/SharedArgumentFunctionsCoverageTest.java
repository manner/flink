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

package org.apache.flink.table.planner.codegen;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Checks that each value of a built-in shared argument function has its {@link
 * SharedArgumentFunctionTestBase} subclass, so that a new function is tested without its own tests
 * for the sharing.
 */
class SharedArgumentFunctionsCoverageTest {

    @Test
    void testEveryValueHasASharingTest() {
        final Map<Class<?>, ?> functions = SharedArgumentFunctionTestBase.sharedFunctionsByValue();
        // PARSE_XML is one of them, so an empty result means that the discovery is broken.
        assertThat(functions).isNotEmpty();
        for (Class<?> value : functions.keySet()) {
            final String testName =
                    SharedArgumentFunctionTestBase.class.getPackage().getName()
                            + "."
                            + value.getSimpleName()
                            + "SharingTest";
            assertThat(testClass(testName))
                    .as("%s needs a test %s", value.getName(), testName)
                    .isNotNull()
                    .isAssignableTo(SharedArgumentFunctionTestBase.class);
        }
    }

    private static Class<?> testClass(String name) {
        try {
            return Class.forName(name);
        } catch (ClassNotFoundException e) {
            return null;
        }
    }
}

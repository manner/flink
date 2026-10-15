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

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.runtime.functions.scalar.SharedArgumentScalarFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Checks that the built-in {@link SharedArgumentScalarFunction}s that prepare the same value share
 * it, in the shapes of queries where the sharing can break.
 *
 * <p>Each prepared value class needs one subclass, named after the value class with the suffix
 * {@code SharingTest}, e.g. {@code XmlDocumentSharingTest}. {@link
 * SharedArgumentFunctionsCoverageTest} fails if it is missing. A subclass only provides the calls
 * and two valid inputs. The results of the calls together are compared with the results of each
 * call alone, so no expected values are needed. The calls must share one preparer, so a second
 * preparer class for the same value fails the test.
 */
abstract class SharedArgumentFunctionTestBase {

    private static final Pattern EVAL_SHARED_PATTERN =
            Pattern.compile(Pattern.quote(".evalShared("));

    // Each shared value creates its preparer once.
    private static final Pattern PREPARER_PATTERN =
            Pattern.compile(Pattern.quote(".createPreparer()"));

    private StreamTableEnvironment tEnv;

    /** The prepared value whose functions this test checks. */
    protected abstract Class<?> valueClass();

    /**
     * Calls of every function that prepares the value, in every shape, with {@code %s} for the
     * shared argument, e.g. {@code "PARSE_XML(%s, TRUE)"}.
     */
    protected abstract List<String> calls();

    /** The type of the shared argument. */
    protected abstract DataType inputType();

    /** A valid input, for which every call returns a non-{@code NULL} result. */
    protected abstract Object input();

    /** Another valid input like {@link #input()}, which gives other results. */
    protected abstract Object otherInput();

    @BeforeEach
    void setUp() {
        tEnv =
                StreamTableEnvironment.create(
                        StreamExecutionEnvironment.getExecutionEnvironment(),
                        EnvironmentSettings.inStreamingMode());
        // A real source, so that the inputs are fields and not constants. Each row has both
        // inputs, so a value kept from the previous row would show.
        final String dataId =
                TestValuesTableFactory.registerData(
                        List.of(
                                Row.of(1, input(), otherInput()),
                                Row.of(2, otherInput(), input())));
        final String type = inputType().getLogicalType().asSerializableString();
        tEnv.executeSql(
                String.format(
                        "CREATE TABLE t (id INT, a %1$s, b %1$s) WITH ('connector' = 'values', "
                                + "'data-id' = '%2$s', 'bounded' = 'true')",
                        type, dataId));
    }

    @AfterEach
    void clearData() {
        TestValuesTableFactory.clearAllData();
    }

    @Test
    void testCallsCoverEveryFunctionOfTheValue() {
        final List<String> functions =
                sharedFunctionsByValue()
                        .getOrDefault(valueClass(), Collections.emptyList())
                        .stream()
                        .map(BuiltInFunctionDefinition::getName)
                        .collect(Collectors.toList());
        assertThat(functions).isNotEmpty();
        for (String function : functions) {
            assertThat(calls()).anyMatch(call -> call.startsWith(function + "("));
        }
    }

    @Test
    void testCallsOnTheSameInputShareTheValue() {
        final List<String> expressions = on("a", calls());
        assertSameResultsAsAlone(expressions);
        assertThat(count(PREPARER_PATTERN, select(expressions))).isOne();
    }

    @Test
    void testDifferentInputsHaveTheirOwnValue() {
        final List<String> expressions = new ArrayList<>(on("a", calls()));
        expressions.addAll(on("b", calls()));
        assertSameResultsAsAlone(expressions);
        assertThat(count(PREPARER_PATTERN, select(expressions))).isEqualTo(2);
    }

    @Test
    void testFilterAndProjectionShareTheValue() {
        final String first = String.format(calls().get(0), "a");
        final String last = String.format(calls().get(calls().size() - 1), "a");
        final String sql = "SELECT id, " + first + " FROM t WHERE " + last + " IS NOT NULL";
        assertThat(GeneratedCodeTestUtils.collect(tEnv, sql)).hasSize(2);
        assertThat(count(PREPARER_PATTERN, sql)).isOne();
    }

    @Test
    void testFirstCallInBranchThatIsNotTaken() {
        // The first call is never taken, so the second one has to prepare the value.
        final String first = String.format(calls().get(0), "a");
        final String last = String.format(calls().get(calls().size() - 1), "a");
        final List<String> expressions =
                List.of("CASE WHEN id > 1000 THEN " + first + " END", last);
        final Map<Integer, Row> results = collectById(select(expressions));
        assertThat(results.values()).allSatisfy(row -> assertThat(row.getField(1)).isNull());
        assertThat(results.values().stream().map(row -> row.getField(2)))
                .containsExactlyInAnyOrderElementsOf(
                        collectById(select(List.of(last))).values().stream()
                                .map(row -> row.getField(1))
                                .collect(Collectors.toList()));
        assertThat(count(EVAL_SHARED_PATTERN, select(expressions)))
                .as("the branch with the first call must survive the optimizer")
                .isEqualTo(2);
        assertThat(count(PREPARER_PATTERN, select(expressions))).isOne();
    }

    @Test
    void testSharingSurvivesCodeSplitting() {
        final List<String> expressions = on("a", calls());
        final Map<Integer, Row> expected = collectById(select(expressions));
        // Only the split code is compiled, so the same results show that the sharing survives it.
        tEnv.getConfig()
                .set(TableConfigOptions.MAX_LENGTH_GENERATED_CODE, 1)
                .set(TableConfigOptions.MAX_MEMBERS_GENERATED_CODE, 1);
        assertThat(collectById(select(expressions))).isEqualTo(expected);
    }

    // --------------------------------------------------------------------------------------------

    /** Returns the built-in shared argument functions, grouped by the value they prepare. */
    static Map<Class<?>, List<BuiltInFunctionDefinition>> sharedFunctionsByValue() {
        final Map<Class<?>, List<BuiltInFunctionDefinition>> functions = new LinkedHashMap<>();
        for (BuiltInFunctionDefinition definition : BuiltInFunctionDefinitions.getDefinitions()) {
            definition
                    .getRuntimeClass()
                    .flatMap(SharedArgumentFunctionTestBase::valueClass)
                    .ifPresent(
                            value ->
                                    functions
                                            .computeIfAbsent(value, v -> new ArrayList<>())
                                            .add(definition));
        }
        return functions;
    }

    /** Returns the value class of a shared argument function, read from its declared types. */
    private static Optional<Class<?>> valueClass(String runtimeClass) {
        final Class<?> functionClass;
        try {
            functionClass = Class.forName(runtimeClass);
        } catch (ClassNotFoundException e) {
            return Optional.empty();
        }
        for (Class<?> c = functionClass; c != null; c = c.getSuperclass()) {
            final Type type = c.getGenericSuperclass();
            if (type instanceof ParameterizedType
                    && ((ParameterizedType) type).getRawType()
                            == SharedArgumentScalarFunction.class) {
                return Optional.of(
                        (Class<?>) ((ParameterizedType) type).getActualTypeArguments()[1]);
            }
        }
        return Optional.empty();
    }

    private static List<String> on(String column, List<String> calls) {
        return calls.stream().map(call -> String.format(call, column)).collect(Collectors.toList());
    }

    private static String select(List<String> expressions) {
        return "SELECT id, " + String.join(", ", expressions) + " FROM t";
    }

    private void assertSameResultsAsAlone(List<String> expressions) {
        final Map<Integer, Row> together = collectById(select(expressions));
        for (int i = 0; i < expressions.size(); i++) {
            final Map<Integer, Row> alone = collectById(select(List.of(expressions.get(i))));
            for (Map.Entry<Integer, Row> row : alone.entrySet()) {
                assertThat(together.get(row.getKey()).getField(i + 1))
                        .as("%s for id %s", expressions.get(i), row.getKey())
                        .isEqualTo(row.getValue().getField(1));
            }
        }
    }

    private Map<Integer, Row> collectById(String sql) {
        final Map<Integer, Row> rows = new HashMap<>();
        for (Row row : GeneratedCodeTestUtils.collect(tEnv, sql)) {
            rows.put((Integer) row.getField(0), row);
        }
        return rows;
    }

    private int count(Pattern pattern, String sql) {
        return GeneratedCodeTestUtils.generatedClassCodes(tEnv, sql).stream()
                .mapToInt(code -> GeneratedCodeTestUtils.countMatches(pattern, code))
                .sum();
    }
}

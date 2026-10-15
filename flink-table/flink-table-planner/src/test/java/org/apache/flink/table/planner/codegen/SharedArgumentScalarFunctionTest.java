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
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.SpecializedFunction;
import org.apache.flink.table.module.Module;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.runtime.functions.scalar.ArgumentPreparer;
import org.apache.flink.table.runtime.functions.scalar.SharedArgumentScalarFunction;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.core.testutils.FlinkAssertions.anyCauseMatches;
import static org.apache.flink.table.functions.FunctionKind.SCALAR;
import static org.apache.flink.table.types.inference.InputTypeStrategies.logical;
import static org.apache.flink.table.types.inference.InputTypeStrategies.sequence;
import static org.apache.flink.table.types.inference.TypeStrategies.explicit;
import static org.apache.flink.table.types.inference.TypeStrategies.nullableIfArgs;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests the mechanism of {@link SharedArgumentScalarFunction}s with test functions, whose preparers
 * count how often they are created, run, and closed.
 *
 * <p>The query shapes in which the sharing can break, e.g. a {@code CASE} branch or code splitting,
 * are checked for each built-in function by {@link SharedArgumentFunctionTestBase}. This test
 * covers what the generated code alone doesn't show: how often a value is prepared at runtime,
 * {@code NULL} inputs, closing, and the errors while planning.
 */
class SharedArgumentScalarFunctionTest {

    private StreamTableEnvironment tEnv;

    @BeforeEach
    void setUp() {
        UpperCase.PREPARED.set(0);
        UpperCase.USED.clear();
        UpperCase.CLOSED.clear();
        LowerCase.PREPARED.set(0);
        tEnv =
                StreamTableEnvironment.create(
                        StreamExecutionEnvironment.getExecutionEnvironment(),
                        EnvironmentSettings.inStreamingMode());
        tEnv.loadModule("shared", new SharedFunctionsModule());
        tEnv.createTemporaryView(
                "t",
                tEnv.fromValues(
                        DataTypes.ROW(
                                DataTypes.FIELD("s", DataTypes.STRING()),
                                DataTypes.FIELD("other", DataTypes.STRING())),
                        Row.of("x", "p"),
                        Row.of("y", "q"),
                        Row.of(null, "r")));
    }

    @AfterEach
    void clearData() {
        TestValuesTableFactory.clearAllData();
    }

    @Test
    void testDifferentFunctionsShareTheValue() {
        assertThat(collect("SELECT SHARED_A(s), SHARED_B(s) FROM t"))
                .containsExactlyInAnyOrder(
                        Row.of("a:X", "b:X"), Row.of("a:Y", "b:Y"), Row.of(null, null));
        // A NULL input isn't prepared.
        assertThat(UpperCase.PREPARED).hasValue(2);
    }

    @Test
    void testPreparersAreClosed() {
        collect("SELECT SHARED_A(s), SHARED_B(s) FROM t");
        // The planner creates preparers only to get their class, so only the used ones matter.
        assertThat(UpperCase.USED).isNotEmpty();
        assertThat(UpperCase.CLOSED).containsAll(UpperCase.USED);
    }

    @Test
    void testDifferentInputsArePreparedSeparately() {
        assertThat(collect("SELECT SHARED_A(s), SHARED_B(other) FROM t"))
                .containsExactlyInAnyOrder(
                        Row.of("a:X", "b:P"), Row.of("a:Y", "b:Q"), Row.of(null, "b:R"));
        assertThat(UpperCase.PREPARED).hasValue(5);
    }

    @Test
    void testDifferentPreparersAreNotShared() {
        assertThat(collect("SELECT SHARED_A(s), OTHER(s) FROM t"))
                .containsExactlyInAnyOrder(
                        Row.of("a:X", "o:x"), Row.of("a:Y", "o:y"), Row.of(null, null));
        assertThat(UpperCase.PREPARED).hasValue(2);
        assertThat(LowerCase.PREPARED).hasValue(2);
    }

    @Test
    void testValueNextToTheSharedJsonParse() {
        // JSON_LENGTH shares its parsed input as well, which must not be mixed up.
        assertThat(
                        collect(
                                "SELECT JSON_LENGTH(j), SHARED_A(j), SHARED_B(j) "
                                        + "FROM (VALUES ('[1,2]'), ('[3]')) AS v(j)"))
                .containsExactlyInAnyOrder(
                        Row.of(2, "a:[1,2]", "b:[1,2]"), Row.of(1, "a:[3]", "b:[3]"));
        assertThat(UpperCase.PREPARED).hasValue(2);
    }

    @Test
    void testValueIsPreparedForEachRowInMatchRecognize() {
        // A matches the first three rows. A value kept from the previous row would give 9.
        final String dataId =
                TestValuesTableFactory.registerData(
                        Arrays.asList(
                                Row.of(1000, "x", Instant.ofEpochMilli(1000L)),
                                Row.of(2000, "yy", Instant.ofEpochMilli(2000L)),
                                Row.of(3000, "zzz", Instant.ofEpochMilli(3000L)),
                                Row.of(9000, "end", Instant.ofEpochMilli(9000L))));
        tEnv.executeSql(
                "CREATE TABLE events (f0 INT, f1 STRING, ts TIMESTAMP_LTZ(3), "
                        + "WATERMARK FOR ts AS ts) WITH ('connector' = 'values', 'data-id' = '"
                        + dataId
                        + "', 'bounded' = 'true')");
        assertThat(
                        collect(
                                "SELECT total FROM events MATCH_RECOGNIZE ("
                                        + " ORDER BY ts"
                                        + " MEASURES SUM(CHARACTER_LENGTH(SHARED_A(A.f1))) AS total"
                                        + " AFTER MATCH SKIP PAST LAST ROW"
                                        + " PATTERN (A+ B)"
                                        + " DEFINE A AS A.f0 < 9000, B AS B.f0 >= 9000)"))
                .containsExactly(Row.of(12));
    }

    @Test
    void testMissingEvalSharedFailsPlanning() {
        assertThatThrownBy(() -> collect("SELECT NO_EVAL_SHARED(s) FROM t"))
                .satisfies(
                        anyCauseMatches(
                                CodeGenException.class,
                                "must have a public method evalShared(String) that returns a "
                                        + "StringData"));
    }

    @Test
    void testPreparerForAnotherArgumentFailsPlanning() {
        assertThatThrownBy(() -> collect("SELECT WRONG_PREPARER(s) FROM t"))
                .satisfies(
                        anyCauseMatches(
                                CodeGenException.class,
                                "prepares a Integer, but the argument is a StringData"));
    }

    @Disabled("Fused operators declare the field of a runtime class function twice")
    @Test
    void testCallsInFusedOperators() {
        // The join and the projection above it are fused into one class, and both call SHARED_A.
        final TableEnvironment bEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        bEnv.loadModule("shared", new SharedFunctionsModule());
        bEnv.getConfig()
                .set(ExecutionConfigOptions.TABLE_EXEC_OPERATOR_FUSION_CODEGEN_ENABLED, true)
                .set(
                        ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS,
                        "NestedLoopJoin,SortMergeJoin")
                .set(OptimizerConfigOptions.TABLE_OPTIMIZER_BROADCAST_JOIN_THRESHOLD, -1L);
        bEnv.createTemporaryView(
                "src", bEnv.fromValues(Row.of(1, "x"), Row.of(2, "y")).as("id", "s"));
        // Different values, so that the planner can't push the join condition below the join.
        bEnv.createTemporaryView(
                "dim", bEnv.fromValues(Row.of(1, 0), Row.of(2, 1)).as("id", "min_length"));
        assertThat(
                        GeneratedCodeTestUtils.collect(
                                bEnv,
                                "SELECT SHARED_A(src.s) FROM src JOIN dim ON src.id = dim.id "
                                        + "AND CHARACTER_LENGTH(SHARED_B(src.s)) > dim.min_length"))
                .containsExactlyInAnyOrder(Row.of("a:X"), Row.of("a:Y"));
    }

    private List<Row> collect(String sql) {
        return GeneratedCodeTestUtils.collect(tEnv, sql);
    }

    // --------------------------------------------------------------------------------------------
    // Test functions
    // --------------------------------------------------------------------------------------------

    private static final BuiltInFunctionDefinition SHARED_A =
            definition("SHARED_A", SharedAFunction.class);
    private static final BuiltInFunctionDefinition SHARED_B =
            definition("SHARED_B", SharedBFunction.class);
    private static final BuiltInFunctionDefinition OTHER = definition("OTHER", OtherFunction.class);
    private static final BuiltInFunctionDefinition NO_EVAL_SHARED =
            definition("NO_EVAL_SHARED", NoEvalSharedFunction.class);
    private static final BuiltInFunctionDefinition WRONG_PREPARER =
            definition("WRONG_PREPARER", WrongPreparerFunction.class);

    private static BuiltInFunctionDefinition definition(String name, Class<?> runtimeClass) {
        return BuiltInFunctionDefinition.newBuilder()
                .name(name)
                .kind(SCALAR)
                .inputTypeStrategy(sequence(logical(LogicalTypeFamily.CHARACTER_STRING)))
                .outputTypeStrategy(nullableIfArgs(explicit(DataTypes.STRING())))
                .runtimeClass(runtimeClass.getName())
                .build();
    }

    /** Provides the test functions. */
    private static final class SharedFunctionsModule implements Module {

        private static final Map<String, FunctionDefinition> FUNCTIONS =
                Stream.of(SHARED_A, SHARED_B, OTHER, NO_EVAL_SHARED, WRONG_PREPARER)
                        .collect(
                                Collectors.toMap(
                                        BuiltInFunctionDefinition::getName,
                                        definition -> definition));

        @Override
        public Set<String> listFunctions() {
            return FUNCTIONS.keySet();
        }

        @Override
        public Optional<FunctionDefinition> getFunctionDefinition(String name) {
            return FUNCTIONS.entrySet().stream()
                    .filter(entry -> entry.getKey().equalsIgnoreCase(name))
                    .map(Map.Entry::getValue)
                    .findFirst();
        }
    }

    /** Prepares the upper case of the argument. */
    public static final class UpperCase implements ArgumentPreparer<StringData, String> {

        static final AtomicInteger PREPARED = new AtomicInteger();

        // The preparers that prepared a value, and the ones that were closed.
        static final Set<UpperCase> USED = ConcurrentHashMap.newKeySet();
        static final Set<UpperCase> CLOSED = ConcurrentHashMap.newKeySet();

        @Override
        public String prepare(StringData argument) {
            PREPARED.incrementAndGet();
            USED.add(this);
            return argument.toString().toUpperCase(Locale.ROOT);
        }

        @Override
        public void close() {
            CLOSED.add(this);
        }
    }

    /** Prepares the lower case of the argument. */
    public static final class LowerCase implements ArgumentPreparer<StringData, String> {

        static final AtomicInteger PREPARED = new AtomicInteger();

        @Override
        public String prepare(StringData argument) {
            PREPARED.incrementAndGet();
            return argument.toString().toLowerCase(Locale.ROOT);
        }
    }

    /** Returns the prepared value with a prefix. */
    private abstract static class PrefixFunction
            extends SharedArgumentScalarFunction<StringData, String> {

        private final String prefix;

        PrefixFunction(
                BuiltInFunctionDefinition definition,
                SpecializedFunction.SpecializedContext context,
                String prefix) {
            super(definition, context);
            this.prefix = prefix;
        }

        public @Nullable StringData evalShared(@Nullable String prepared) {
            return prepared == null ? null : StringData.fromString(prefix + prepared);
        }
    }

    /** Shares the upper case with {@link SharedBFunction}. */
    public static final class SharedAFunction extends PrefixFunction {
        public SharedAFunction(SpecializedFunction.SpecializedContext context) {
            super(SHARED_A, context, "a:");
        }

        @Override
        public ArgumentPreparer<StringData, String> createPreparer() {
            return new UpperCase();
        }
    }

    /** Shares the upper case with {@link SharedAFunction}. */
    public static final class SharedBFunction extends PrefixFunction {
        public SharedBFunction(SpecializedFunction.SpecializedContext context) {
            super(SHARED_B, context, "b:");
        }

        @Override
        public ArgumentPreparer<StringData, String> createPreparer() {
            return new UpperCase();
        }
    }

    /** Uses another preparer, so it shares nothing with the others. */
    public static final class OtherFunction extends PrefixFunction {
        public OtherFunction(SpecializedFunction.SpecializedContext context) {
            super(OTHER, context, "o:");
        }

        @Override
        public ArgumentPreparer<StringData, String> createPreparer() {
            return new LowerCase();
        }
    }

    /** Has an eval method instead of evalShared. */
    public static final class NoEvalSharedFunction
            extends SharedArgumentScalarFunction<StringData, String> {

        public NoEvalSharedFunction(SpecializedFunction.SpecializedContext context) {
            super(NO_EVAL_SHARED, context);
        }

        @Override
        public ArgumentPreparer<StringData, String> createPreparer() {
            return new UpperCase();
        }

        public @Nullable StringData eval(@Nullable StringData s) {
            return s;
        }
    }

    /** Prepares an integer argument. */
    public static final class IntegerLength implements ArgumentPreparer<Integer, String> {

        @Override
        public String prepare(Integer argument) {
            return String.valueOf(argument);
        }
    }

    /** Has a preparer for an integer, but gets a string. */
    public static final class WrongPreparerFunction
            extends SharedArgumentScalarFunction<Integer, String> {

        public WrongPreparerFunction(SpecializedFunction.SpecializedContext context) {
            super(WRONG_PREPARER, context);
        }

        @Override
        public ArgumentPreparer<Integer, String> createPreparer() {
            return new IntegerLength();
        }

        public @Nullable StringData evalShared(@Nullable String prepared) {
            return StringData.fromString(prepared);
        }
    }
}

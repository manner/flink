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

package org.apache.flink.table.planner.codegen.calls;

import org.apache.flink.table.planner.codegen.CodeGenException;
import org.apache.flink.table.planner.codegen.CodeGenUtils;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.GeneratedExpression;
import org.apache.flink.table.runtime.functions.scalar.ArgumentPreparer;
import org.apache.flink.table.runtime.functions.scalar.SharedArgumentScalarFunction;
import org.apache.flink.table.types.extraction.ExtractionUtils;
import org.apache.flink.table.types.logical.utils.LogicalTypeUtils;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import scala.Option;

/**
 * Code generation of the first argument of a {@link SharedArgumentScalarFunction}.
 *
 * <p>For example, {@code SELECT PARSE_XML(xml), TRY_PARSE_XML(xml, TRUE) FROM t} generates code
 * similar to:
 *
 * <pre>{@code
 * // members
 * XmlDocumentReader xmlDocumentReader$0;
 * BinaryStringData xmlDocumentReaderInput$1;
 * XmlDocument xmlDocumentReaderValue$2;
 *
 * private XmlDocument prepareXmlDocumentReader$3(BinaryStringData in) {
 *   if (in == null) {
 *     return null;
 *   }
 *   if (in != xmlDocumentReaderInput$1) {
 *     if (xmlDocumentReader$0 == null) {
 *       xmlDocumentReader$0 = (XmlDocumentReader) parseXmlFunction.createPreparer();
 *     }
 *     xmlDocumentReaderInput$1 = in;
 *     xmlDocumentReaderValue$2 = xmlDocumentReader$0.prepare(in);
 *   }
 *   return xmlDocumentReaderValue$2;
 * }
 *
 * // calls
 * result$4 = parseXmlFunction.evalShared(prepareXmlDocumentReader$3(isNull$0 ? null : field$0));
 * result$5 = tryParseXmlFunction.evalShared(prepareXmlDocumentReader$3(...), true);
 * }</pre>
 *
 * <p>The input is a new object for each record, so the value is prepared again when it changes.
 */
public final class SharedArgumentCodeGenUtils {

    public static final String EVAL_SHARED = "evalShared";

    private SharedArgumentCodeGenUtils() {}

    /**
     * Returns the first argument of the call, with the value that the calls with the same preparer
     * class on the same input share.
     *
     * @param otherArgumentClasses the classes of the other arguments that {@code evalShared} gets
     * @param resultClass the class that {@code evalShared} must return
     */
    public static GeneratedExpression generateSharedArgument(
            CodeGeneratorContext ctx,
            SharedArgumentScalarFunction<?, ?> function,
            String functionTerm,
            String functionName,
            GeneratedExpression argument,
            List<Class<?>> otherArgumentClasses,
            Class<?> resultClass) {
        final Class<?> preparerClass = function.getPreparerClass();
        final PreparerTypes types = preparerTypes(preparerClass);
        checkPreparer(preparerClass, types, argument);
        checkEvalShared(function, functionName, types.value, otherArgumentClasses, resultClass);
        final String input = argument.nullTerm() + " ? null : " + argument.resultTerm();
        final String method =
                prepareMethod(ctx, functionTerm, preparerClass, types.value, argument);
        return new GeneratedExpression(
                method + "(" + input + ")",
                argument.nullTerm(),
                argument.code(),
                argument.resultType(),
                Option.empty());
    }

    /** Returns the method that prepares the value, which the first call on the input adds. */
    private static String prepareMethod(
            CodeGeneratorContext ctx,
            String functionTerm,
            Class<?> preparerClass,
            Class<?> valueClass,
            GeneratedExpression argument) {
        final String key = preparerClass.getName() + "(" + argument.resultTerm() + ")";
        return ctx.getOrAddReusableSharedValue(
                key,
                () -> addPrepareMethod(ctx, functionTerm, preparerClass, valueClass, argument));
    }

    /**
     * Adds the method that prepares the value. The preparer comes from the function of the first
     * call, and it is created on first use, since the function is set in the constructor.
     */
    private static String addPrepareMethod(
            CodeGeneratorContext ctx,
            String functionTerm,
            Class<?> preparerClass,
            Class<?> valueClass,
            GeneratedExpression argument) {
        final String preparerType = preparerClass.getCanonicalName();
        final String valueType = valueClass.getCanonicalName();
        final String inputType = CodeGenUtils.boxedTypeTermForType(argument.resultType());
        final String name = preparerClass.getSimpleName();
        final String preparerTerm =
                CodeGenUtils.newName(
                        ctx, Character.toLowerCase(name.charAt(0)) + name.substring(1));
        final String inputTerm = CodeGenUtils.newName(ctx, preparerTerm + "Input");
        final String valueTerm = CodeGenUtils.newName(ctx, preparerTerm + "Value");
        final String methodTerm = CodeGenUtils.newName(ctx, "prepare" + name);

        ctx.addReusableMember(String.format("%s %s;", preparerType, preparerTerm));
        ctx.addReusableCloseStatement(
                String.format("if (%1$s != null) {\n  %1$s.close();\n}", preparerTerm));
        ctx.addReusableMember(String.format("%s %s;", inputType, inputTerm));
        ctx.addReusableMember(String.format("%s %s;", valueType, valueTerm));
        ctx.addReusableMember(
                String.format(
                        "private %1$s %2$s(%3$s in) {\n"
                                + "  if (in == null) {\n"
                                + "    return null;\n"
                                + "  }\n"
                                + "  if (in != %4$s) {\n"
                                + "    if (%6$s == null) {\n"
                                + "      %6$s = (%7$s) %8$s.createPreparer();\n"
                                + "    }\n"
                                + "    %4$s = in;\n"
                                + "    %5$s = %6$s.prepare(in);\n"
                                + "  }\n"
                                + "  return %5$s;\n"
                                + "}\n",
                        valueType,
                        methodTerm,
                        inputType,
                        inputTerm,
                        valueTerm,
                        preparerTerm,
                        preparerType,
                        functionTerm));
        return methodTerm;
    }

    /** The argument and the value class of a preparer, as it declares them. */
    private static final class PreparerTypes {
        private final Class<?> argument;
        private final Class<?> value;

        private PreparerTypes(Class<?> argument, Class<?> value) {
            this.argument = argument;
            this.value = value;
        }
    }

    /**
     * Returns the type arguments of {@link ArgumentPreparer} that the preparer class or one of its
     * superclasses declares.
     */
    private static PreparerTypes preparerTypes(Class<?> preparerClass) {
        for (Class<?> c = preparerClass; c != null; c = c.getSuperclass()) {
            for (Type type : c.getGenericInterfaces()) {
                if (type instanceof ParameterizedType
                        && ((ParameterizedType) type).getRawType() == ArgumentPreparer.class) {
                    final Type[] arguments = ((ParameterizedType) type).getActualTypeArguments();
                    if (arguments[0] instanceof Class && arguments[1] instanceof Class) {
                        return new PreparerTypes((Class<?>) arguments[0], (Class<?>) arguments[1]);
                    }
                }
            }
        }
        throw new CodeGenException(
                String.format(
                        "%s must implement %s with concrete classes for the argument and the "
                                + "value.",
                        preparerClass.getName(), ArgumentPreparer.class.getSimpleName()));
    }

    /**
     * Fails if the generated code can't refer to the preparer, or if the preparer doesn't accept
     * the argument.
     */
    private static void checkPreparer(
            Class<?> preparerClass, PreparerTypes types, GeneratedExpression argument) {
        if (!Modifier.isPublic(preparerClass.getModifiers())) {
            throw new CodeGenException(
                    String.format("%s must be public.", preparerClass.getName()));
        }
        final Class<?> argumentClass =
                LogicalTypeUtils.toInternalConversionClass(argument.resultType());
        if (!types.argument.isAssignableFrom(argumentClass)) {
            throw new CodeGenException(
                    String.format(
                            "%s prepares a %s, but the argument is a %s.",
                            preparerClass.getName(),
                            types.argument.getSimpleName(),
                            argumentClass.getSimpleName()));
        }
    }

    /**
     * Fails if the function has no evalShared method that takes the value and the arguments, and
     * returns the result class.
     */
    private static void checkEvalShared(
            SharedArgumentScalarFunction<?, ?> function,
            String functionName,
            Class<?> valueClass,
            List<Class<?>> otherArgumentClasses,
            Class<?> resultClass) {
        final boolean found =
                Arrays.stream(function.getClass().getMethods())
                        .filter(method -> method.getName().equals(EVAL_SHARED))
                        .filter(method -> accepts(method, valueClass, otherArgumentClasses))
                        .anyMatch(
                                method ->
                                        resultClass.isAssignableFrom(
                                                ExtractionUtils.primitiveToWrapper(
                                                        method.getReturnType())));
        if (!found) {
            throw new CodeGenException(
                    String.format(
                            "Function %s must have a public method %s(%s) that returns a %s.",
                            functionName,
                            EVAL_SHARED,
                            Stream.concat(Stream.of(valueClass), otherArgumentClasses.stream())
                                    .map(Class::getSimpleName)
                                    .collect(Collectors.joining(", ")),
                            resultClass.getSimpleName()));
        }
    }

    /** The parameters must take the classes as they are, so NULL arguments need boxed types. */
    private static boolean accepts(
            Method method, Class<?> valueClass, List<Class<?>> otherArgumentClasses) {
        final Class<?>[] parameters = method.getParameterTypes();
        if (parameters.length != otherArgumentClasses.size() + 1
                || !parameters[0].isAssignableFrom(valueClass)) {
            return false;
        }
        for (int i = 0; i < otherArgumentClasses.size(); i++) {
            if (!parameters[i + 1].isAssignableFrom(otherArgumentClasses.get(i))) {
                return false;
            }
        }
        return true;
    }
}

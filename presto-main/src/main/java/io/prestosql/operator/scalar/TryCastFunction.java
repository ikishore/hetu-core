/*
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
package io.prestosql.operator.scalar;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Primitives;
import io.prestosql.metadata.BoundVariables;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.SqlScalarFunction;
import io.prestosql.spi.function.FunctionKind;
import io.prestosql.spi.function.ScalarFunctionImplementation;
import io.prestosql.spi.function.ScalarFunctionImplementation.ArgumentProperty;
import io.prestosql.spi.function.Signature;
import io.prestosql.spi.type.Type;

import java.lang.invoke.MethodHandle;
import java.util.List;

import static io.prestosql.spi.function.Signature.typeVariable;
import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;
import static java.lang.invoke.MethodHandles.catchException;
import static java.lang.invoke.MethodHandles.constant;
import static java.lang.invoke.MethodHandles.dropArguments;
import static java.lang.invoke.MethodType.methodType;

public class TryCastFunction
        extends SqlScalarFunction
{
    public static final TryCastFunction TRY_CAST = new TryCastFunction();

    public TryCastFunction()
    {
        super(new Signature(
                "TRY_CAST",
                FunctionKind.SCALAR,
                ImmutableList.of(typeVariable("F"), typeVariable("T")),
                ImmutableList.of(),
                parseTypeSignature("T"),
                ImmutableList.of(parseTypeSignature("F")),
                false));
    }

    @Override
    public boolean isHidden()
    {
        return true;
    }

    @Override
    public boolean isDeterministic()
    {
        return true;
    }

    @Override
    public String getDescription()
    {
        return "";
    }

    @Override
    public ScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, Metadata metadata)
    {
        Type fromType = boundVariables.getTypeVariable("F");
        Type toType = boundVariables.getTypeVariable("T");

        Class<?> returnType = Primitives.wrap(toType.getJavaType());
        List<ArgumentProperty> argumentProperties;
        MethodHandle tryCastHandle;

        // the resulting method needs to return a boxed type
        Signature signature = metadata.getCoercion(fromType.getTypeSignature(), toType.getTypeSignature());
        ScalarFunctionImplementation implementation = metadata.getScalarFunctionImplementation(signature);
        argumentProperties = ImmutableList.of(implementation.getArgumentProperty(0));
        MethodHandle coercion = implementation.getMethodHandle();
        coercion = coercion.asType(methodType(returnType, coercion.type()));

        MethodHandle exceptionHandler = dropArguments(constant(returnType, null), 0, RuntimeException.class);
        tryCastHandle = catchException(coercion, RuntimeException.class, exceptionHandler);

        return new ScalarFunctionImplementation(true, argumentProperties, tryCastHandle, isDeterministic());
    }
}

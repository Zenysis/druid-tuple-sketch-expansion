/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.query.aggregation.datasketches.tuple.expansion;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.Expr.ObjectBinding;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.math.expr.Parser;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.utils.CollectionUtils;

import java.util.Map;
import java.util.Set;

/**
 * Class that holds a mathematical expression and enables evaluation of that
 * expression over a tuple value (array of doubles).
 */
public class TupleExpressionHolder
{
  private final String expression;

  private final ExprMacroTable macroTable;
  private final Map<String, Function<Object, Object>> finalizers;
  private final Supplier<Expr> parsed;
  private final Supplier<Set<String>> dependentFields;

  public TupleExpressionHolder(String expression)
  {
    this(
        expression,
        ExprMacroTable.nil(),
        ImmutableMap.of()
    );
  }

  // Constructor for `decorate` method.
  // NOTE(stephen): These private constructors roughly match the
  // `ExpressionPostAggregator` structure and try to take advantage of the same
  // performance improvements implemented there.
  private TupleExpressionHolder(
      final String expression,
      final ExprMacroTable macroTable,
      final Map<String, Function<Object, Object>> finalizers
  )
  {
    this(
        expression,
        macroTable,
        finalizers,
        Suppliers.memoize(() -> Parser.parse(expression, macroTable))
    );
  }

  private TupleExpressionHolder(
      final String expression,
      final ExprMacroTable macroTable,
      final Map<String, Function<Object, Object>> finalizers,
      final Supplier<Expr> parsed
  )
  {
    this(
        expression,
        macroTable,
        finalizers,
        parsed,
        Suppliers.memoize(() -> {
          final Set<String> fields =
              parsed.get().analyzeInputs().getRequiredBindings();
          final Set<String> output =
              Sets.newHashSetWithExpectedSize(fields.size());

          for (final String field : fields) {
            if (!field.startsWith("$")) {
              output.add(field);
            }
          }
          return output;
        })
    );
  }

  private TupleExpressionHolder(
      final String expression,
      final ExprMacroTable macroTable,
      final Map<String, Function<Object, Object>> finalizers,
      final Supplier<Expr> parsed,
      final Supplier<Set<String>> dependentFields
  )
  {
    this.expression = Preconditions.checkNotNull(
        expression,
        "Expression string cannot be null"
    );
    this.macroTable = macroTable;
    this.finalizers = finalizers;
    this.parsed = parsed;
    this.dependentFields = dependentFields;
  }

  public double computeDouble(
      final double[] tupleValues,
      final Map<String, Object> combinedAggregators
  )
  {
    return compute(tupleValues, combinedAggregators).asDouble();
  }

  public boolean computeBoolean(
      final double[] tupleValues,
      final Map<String, Object> combinedAggregators
  )
  {
    return compute(tupleValues, combinedAggregators).asBoolean();
  }

  protected ExprEval compute(
      final double[] tupleValues,
      final Map<String, Object> combinedAggregators
  )
  {
    // Build a map of the tuple variables.
    final Map<String, Object> variables =
        Maps.newHashMapWithExpectedSize(tupleValues.length);

    // Since the Druid expression does not support array indexing syntax,
    // we require the user to use `$idx` as the variable names in their
    // expressions.
    for (int i = 0; i < tupleValues.length; i++) {
      variables.put("$" + i, tupleValues[i]);
    }

    return parsed.get().eval(new ObjectBinding() {
        @Override
        public ExpressionType getType(String name)
        {
          return null;
        }

        @Override
        public Object get(String name)
        {
          if (variables.containsKey(name)) {
            return variables.get(name);
          }
          return combinedAggregators.get(name);
        }
      }
    );
  }

  // Memoize the most recent combinedAggregators received.
  public Map<String, Object> memoizeCombinedAggregators(
        final Map<String, Object> combinedAggregators
  )
  {
    // NOTE(stephen): Basing this off the `ExpressionPostAggregator.compute`
    // method. Maps.transformEntries is lazy, will only finalize values we
    // actually read.
    return Maps.transformEntries(
        combinedAggregators,
        (String k, Object v) -> {
          final Function<Object, Object> finalizer = finalizers.get(k);
          return finalizer != null ? finalizer.apply(v) : v;
        }
    );
  }

  public TupleExpressionHolder decorate(
      final Map<String, AggregatorFactory> aggregators
  )
  {
    return new TupleExpressionHolder(
        expression,
        macroTable,
        CollectionUtils.mapValues(
            aggregators,
            aggregatorFactory -> obj -> aggregatorFactory.finalizeComputation(obj)
        ),
        parsed,
        dependentFields
    );
  }

  public Set<String> getDependentFields()
  {
    return dependentFields.get();
  }
}

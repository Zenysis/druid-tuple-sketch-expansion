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

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.Maps;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.Parser;

import java.util.Map;

/**
 * Class that holds a mathematical expression and enables evaluation of that
 * expression over a tuple value (array of doubles).
 */
public class TupleExpressionHolder
{
  private final String expression;

  private final ExprMacroTable macroTable;
  private final Supplier<Expr> parsed;

  public TupleExpressionHolder(String expression)
  {
    this.expression = Preconditions.checkNotNull(
        expression,
        "Expression string cannot be null"
    );
    this.macroTable = ExprMacroTable.nil();
    this.parsed = Suppliers.memoize(
        () -> Parser.parse(this.expression, this.macroTable)
    );
  }

  public double computeDouble(double[] values)
  {
    return compute(values).asDouble();
  }

  public boolean computeBoolean(double[] values)
  {
    return compute(values).asBoolean();
  }

  protected ExprEval compute(double[] values)
  {
    // Since the Druid expression does not support array indexing syntax,
    // we require the user to use `$idx` as the variable names in their
    // expressions.
    final Map<String, Object> variables = Maps.newHashMapWithExpectedSize(
        values.length
    );
    for (int i = 0; i < values.length; i++) {
      variables.put("$" + i, values[i]);
    }
    return parsed.get().eval(Parser.withMap(variables));
  }
}

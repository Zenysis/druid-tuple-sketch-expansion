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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.PostAggregator;

import javax.annotation.Nullable;
import java.util.Map;

/**
 * Filter values out of the sketch by applying the expression to each tuple in
 * the sketch.
 */
public class ArrayOfDoublesFilterExpressionPostAggregator extends ArrayOfDoublesExpressionPostAggregator
{
  public static final byte CACHE_TYPE_ID = 0x76;

  @JsonCreator
  public ArrayOfDoublesFilterExpressionPostAggregator(
      @JsonProperty("name") final String name,
      @JsonProperty("field") final PostAggregator field,
      @JsonProperty("expression") final String expression,
      @JsonProperty("nominalEntries") @Nullable final Integer nominalEntries
  )
  {
    this(name, field, expression, nominalEntries, null);
  }

  // Constructor for `decorate` method.
  private ArrayOfDoublesFilterExpressionPostAggregator(
      final String name,
      final PostAggregator field,
      final String expression,
      @Nullable final Integer nominalEntries,
      @Nullable final TupleExpressionHolder tupleExpression
  )
  {
    super(name, field, expression, nominalEntries, tupleExpression);
  }

  @Override
  @Nullable
  public double[] evaluate(
      final double[] tupleValues,
      final Map<String, Object> combinedAggregators
  )
  {
    // If the expression evaluates to `false`, return `null` to signify that the
    // key that holds these values should be removed from the sketch.
    if (tupleExpression.computeBoolean(tupleValues, combinedAggregators)) {
      return tupleValues;
    }
    return null;
  }

  @Override
  public ArrayOfDoublesFilterExpressionPostAggregator decorate(
      final Map<String, AggregatorFactory> aggregators
  )
  {
    return new ArrayOfDoublesFilterExpressionPostAggregator(
        getName(),
        getField(),
        expression,
        nominalEntries,
        tupleExpression.decorate(aggregators)
    );
  }

  @Override
  public byte[] getCacheKey()
  {
    return buildCacheKey(CACHE_TYPE_ID);
  }
}

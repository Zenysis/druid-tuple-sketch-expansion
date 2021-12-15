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
import org.apache.datasketches.ResizeFactor;
import org.apache.datasketches.Util;
import org.apache.datasketches.tuple.arrayofdoubles.ArrayOfDoublesSketch;
import org.apache.datasketches.tuple.arrayofdoubles.ArrayOfDoublesSketchIterator;
import org.apache.datasketches.tuple.arrayofdoubles.ArrayOfDoublesUpdatableSketch;
import org.apache.datasketches.tuple.arrayofdoubles.ArrayOfDoublesUpdatableSketchBuilder;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.aggregation.datasketches.tuple.ArrayOfDoublesSketchAggregatorFactory;
import org.apache.druid.query.aggregation.datasketches.tuple.ArrayOfDoublesSketchUnaryPostAggregator;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.segment.column.ValueType;

import javax.annotation.Nullable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;

/**
 * Apply an expression on each tuple in the sketch.
 */
public abstract class ArrayOfDoublesExpressionPostAggregator extends ArrayOfDoublesSketchUnaryPostAggregator
{
  private final Logger log = new Logger(ArrayOfDoublesExpressionPostAggregator.class);

  protected final String expression;
  protected final TupleExpressionHolder tupleExpression;
  protected final int nominalEntries;

  @JsonCreator
  public ArrayOfDoublesExpressionPostAggregator(
      final String name,
      final PostAggregator field,
      final String expression,
      @Nullable final Integer nominalEntries,
      @Nullable final TupleExpressionHolder tupleExpression
  )
  {
    super(name, field);
    this.expression = expression;
    this.nominalEntries =
        nominalEntries == null ? Util.DEFAULT_NOMINAL_ENTRIES : nominalEntries;
    this.tupleExpression =
        tupleExpression == null ? new TupleExpressionHolder(expression) : tupleExpression;
    Util.checkIfPowerOf2(this.nominalEntries, "nominalEntries");
  }

  @JsonProperty
  public String getExpression()
  {
    return expression;
  }

  @JsonProperty
  public int getNominalEntries()
  {
    return nominalEntries;
  }

  // Evaluate the expression over the tuple provided. Return an array to store
  // in the updated sketch. If `null` is returned, the key associated with this
  // tuple will be removed from the sketch.
  @Nullable
  abstract double[] evaluate(
      double[] values,
      Map<String, Object> combinedAggregators
  );

  @Override
  public ArrayOfDoublesSketch compute(final Map<String, Object> combinedAggregators)
  {
    final ArrayOfDoublesSketch input =
        (ArrayOfDoublesSketch) getField().compute(combinedAggregators);
    final ArrayOfDoublesSketchIterator it = input.iterator();

    final Map<String, Object> memoizedCombinedAggregators =
        tupleExpression.memoizeCombinedAggregators(combinedAggregators);

    // NOTE(stephen): This is essentially the `ArrayOfDoublesIntersection`
    // `update` method tailored to this situation where values can be filtered
    // out during *evaluation* instead of if the key is missing in the input
    // sketch.
    final int inputRetainedEntries = input.getRetainedEntries();
    final long[] matchKeys = new long[inputRetainedEntries];
    final double[][] matchValues = new double[inputRetainedEntries][];
    int matchCount = 0;
    int numberOfValues = -1;
    while (it.next()) {
      final long key = it.getKey();
      final double[] values = it.getValues();
      if (values != null) {
        final double[] result = evaluate(values, memoizedCombinedAggregators);
        if (result != null) {
          matchKeys[matchCount] = key;
          matchValues[matchCount] = result;
          matchCount++;

          if (numberOfValues == -1) {
            numberOfValues = result.length;
          }
        }
      }
    }
    final ArrayOfDoublesUpdatableSketch output =
        new ArrayOfDoublesUpdatableSketchBuilder()
            .setNominalEntries(matchCount > 16 ? matchCount : 16)
            .setNumberOfValues(numberOfValues > 0 ? numberOfValues : 1)
            .setResizeFactor(ResizeFactor.X1)
            .setSamplingProbability(1f)
            .build();

    // If there are no matches, return an empty sketch.
    if (matchCount == 0) {
      return output.compact();
    }

    // HACK(stephen): GIANT ENORMOUS HACK. The datasketches library is annoying
    // in that most of the concrete classes are shielded and cannot be imported.
    // This means implementing a method (like filtering or direct setting) on
    // top of a ArrayOfDoublesSketch is painful. We must use reflecttion to
    // access these properties.
    try {
      final Class<?> clazz = output.getClass();

      // `insert` is declared on the abstract parent class
      // `ArrayOfDoublesQuickSelectSketch`.
      final Method insert = clazz.getSuperclass().getDeclaredMethod(
          "insert",
          long.class,
          double[].class
      );
      insert.setAccessible(true);

      // `getThetaLong` is declared on `ArrayOfDoublesSketch`.
      final Method getThetaLong =
          ArrayOfDoublesSketch.class.getDeclaredMethod("getThetaLong");
      getThetaLong.setAccessible(true);

      // `setThetaLong` and `setNotEmpty` are implemented on the concrete class
      // `HeapArrayOfDoublesQuickSelectSketch`.
      final Method setThetaLong = clazz.getDeclaredMethod(
          "setThetaLong",
          long.class
      );
      setThetaLong.setAccessible(true);

      final Method setNotEmpty = clazz.getDeclaredMethod("setNotEmpty");
      setNotEmpty.setAccessible(true);

      for (int i = 0; i < matchCount; i++) {
        insert.invoke(output, matchKeys[i], matchValues[i]);
      }
      setThetaLong.invoke(output, getThetaLong.invoke(input));
      setNotEmpty.invoke(output);

      return output.compact();
    }
    catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
      // TODO(stephen): Do something better here.
      log.warn(e, "EXCEPTION: " + e);
    }

    return output.compact();
  }

  @Override
  public Comparator<ArrayOfDoublesSketch> getComparator()
  {
    return ArrayOfDoublesSketchAggregatorFactory.COMPARATOR;
  }

  @Override
  public Set<String> getDependentFields()
  {
    return tupleExpression.getDependentFields();
  }

  /**
   * actual type is {@link ArrayOfDoublesSketch}
   */
  @Override
  public ValueType getType()
  {
    return ValueType.COMPLEX;
  }

  @Override
  public boolean equals(final Object o)
  {
    if (!super.equals(0)) {
      return false;
    }

    final ArrayOfDoublesExpressionPostAggregator that = (ArrayOfDoublesExpressionPostAggregator) o;
    return getField().equals(that.getField()) &&
        expression.equals(that.getExpression()) &&
        nominalEntries == that.getNominalEntries();
  }

  @Override
  public String toString()
  {
    return this.getClass().getSimpleName() + "{" +
        "name='" + getName() + "'" +
        ", field=" + getField() +
        ", expression='" + getExpression() + "'" +
        ", nominalEntries=" + getNominalEntries() +
        "}";
  }

  protected byte[] buildCacheKey(final byte cacheTypeId)
  {
    return new CacheKeyBuilder(cacheTypeId)
        .appendCacheable(getField())
        .appendString(expression)
        .appendInt(nominalEntries)
        .build();
  }
}

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

import org.apache.datasketches.tuple.ArrayOfDoublesSketch;
import org.apache.datasketches.tuple.ArrayOfDoublesUpdatableSketch;
import org.apache.datasketches.tuple.ArrayOfDoublesUpdatableSketchBuilder;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.aggregation.TestObjectColumnSelector;
import org.apache.druid.query.aggregation.datasketches.tuple.ArrayOfDoublesSketchMergeAggregator;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class ArrayOfDoublesCollapseExpressionPostAggregatorTest extends InitializedNullHandlingTest
{
  public Aggregator buildAggregator()
  {
    ArrayOfDoublesUpdatableSketch sketch1 =
        new ArrayOfDoublesUpdatableSketchBuilder()
        .setNominalEntries(1024)
        .setNumberOfValues(3)
        .build();
    sketch1.update("a", new double[] {1, 2, 3});
    sketch1.update("b", new double[] {2, 2, 3});
    sketch1.update("c", new double[] {3, 3, 3});

    TestObjectColumnSelector<ArrayOfDoublesSketch> selector =
        new TestObjectColumnSelector<ArrayOfDoublesSketch>(
            new ArrayOfDoublesSketch[] {sketch1}
        );
    Aggregator aggregator = new ArrayOfDoublesSketchMergeAggregator(
        selector,
        1024,
        3
    );
    aggregator.aggregate();
    return aggregator;
  }

  public ArrayOfDoublesSketch evaluateCollapseExpression(String expression)
  {
    PostAggregator fieldAccessPostAgg = new FieldAccessPostAggregator(
        "testName",
        "field"
    );
    Aggregator agg = buildAggregator();
    Map<String, Object> metricValues = new HashMap<String, Object>();
    metricValues.put("field", agg.get());

    ArrayOfDoublesCollapseExpressionPostAggregator postAgg =
        new ArrayOfDoublesCollapseExpressionPostAggregator(
            "postAgg",
            fieldAccessPostAgg,
            expression,
            1024
        );
    return postAgg.compute(metricValues);
  }

  @Test
  public void testFilterExpression()
  {
    ArrayOfDoublesSketch sketchA = evaluateCollapseExpression("$0 + $1 + $2");
    assertCollapsedValues(sketchA, 6d, 7d, 9d);
  }

  public void assertCollapsedValues(
      final ArrayOfDoublesSketch sketch,
      double... expectedVals
  )
  {
    final double[][] entries = sketch.getValues();
    final int retainedEntries = entries.length;
    Assert.assertEquals(expectedVals.length, retainedEntries);

    double[] sortedExpectedVals = expectedVals.clone();
    Arrays.sort(sortedExpectedVals);

    double[] sortedReceivedVals = new double[retainedEntries];
    for (int i = 0; i < retainedEntries; i++) {
      final double[] values = entries[i];
      Assert.assertEquals(1, values.length);
      sortedReceivedVals[i] = values[0];
    }

    Arrays.sort(sortedReceivedVals);
    Assert.assertArrayEquals(sortedExpectedVals, sortedReceivedVals, 0.001);
  }
}

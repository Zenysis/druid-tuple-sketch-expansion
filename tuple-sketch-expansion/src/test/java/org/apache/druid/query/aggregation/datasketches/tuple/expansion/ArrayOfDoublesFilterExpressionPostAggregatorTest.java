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

import java.util.HashMap;
import java.util.Map;

public class ArrayOfDoublesFilterExpressionPostAggregatorTest extends InitializedNullHandlingTest
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

  public ArrayOfDoublesSketch evaluateFilterExpression(String expression)
  {
    PostAggregator fieldAccessPostAgg = new FieldAccessPostAggregator(
        "testName",
        "field"
    );
    Aggregator agg = buildAggregator();
    Map<String, Object> metricValues = new HashMap<String, Object>();
    metricValues.put("field", agg.get());

    ArrayOfDoublesFilterExpressionPostAggregator postAgg =
        new ArrayOfDoublesFilterExpressionPostAggregator(
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
    ArrayOfDoublesSketch sketchA = evaluateFilterExpression("$0 > 0");
    ArrayOfDoublesSketch sketchB = evaluateFilterExpression("$1 == 2");
    ArrayOfDoublesSketch sketchC = evaluateFilterExpression("$2 > 3");
    ArrayOfDoublesSketch sketchD = evaluateFilterExpression("$0 < $1 && $2 == 3");
    Assert.assertEquals(3d, sketchA.getEstimate(), 0.001);
    Assert.assertEquals(2d, sketchB.getEstimate(), 0.001);
    Assert.assertEquals(0d, sketchC.getEstimate(), 0.001);
    Assert.assertEquals(1d, sketchD.getEstimate(), 0.001);
  }
}

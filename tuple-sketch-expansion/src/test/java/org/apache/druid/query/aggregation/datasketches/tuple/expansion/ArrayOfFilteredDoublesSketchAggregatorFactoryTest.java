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

import com.google.common.base.Predicate;
import org.apache.datasketches.tuple.ArrayOfDoublesSketch;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.TestFloatColumnSelector;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.AbstractDimensionSelector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.DimensionSelectorUtils;
import org.apache.druid.segment.IdLookup;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.data.SingleIndexedInt;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ArrayOfFilteredDoublesSketchAggregatorFactoryTest extends InitializedNullHandlingTest
{
  @Test
  public void testFactorize()
  {
    // Columns of a hypothetical table.
    final String[] sketchDimensionValues = {
        "test_a", "test_a", "test_b", "test_b", "test_c", "test_c"
    };
    final String[] filterDimensionValues = {
        "f_1", "f_2", "f_3", "f_1", "f_2", "f_3"
    };
    final float[] values = {1, 2, 3, 4, 5, 6};
    List<MetricFilterSpec> metricFilters = new ArrayList<>();
    metricFilters.add(buildMetricFilterSpec("f_1"));
    metricFilters.add(buildMetricFilterSpec("f_2"));
    metricFilters.add(buildMetricFilterSpec("f_3"));

    // Final sketch should be:
    // test_a -> [1, 2, 0]
    // test_b -> [4, 0, 3]
    // test_c -> [0, 5, 6]
    final TestFloatColumnSelector valueSelector = new TestFloatColumnSelector(values);
    final DimensionSelector sketchDimensionSelector =
        buildBaseDimensionSelector(sketchDimensionValues, valueSelector);
    final DimensionSelector filterDimensionSelector =
        buildBaseDimensionSelector(filterDimensionValues, valueSelector);
    ColumnSelectorFactory columnSelector = makeColumnSelector(
        sketchDimensionSelector,
        filterDimensionSelector,
        valueSelector
    );

    AggregatorFactory aggFactory = new ArrayOfFilteredDoublesSketchAggregatorFactory(
        "test",
        "sketchDim",
        1024,
        metricFilters,
        metricFilters.size()
    );
    Aggregator agg = aggFactory.factorize(columnSelector);

    // Seed the aggregator with values.
    for (int i = 0; i < values.length; i++) {
      agg.aggregate();
      valueSelector.increment();
    }

    ArrayOfDoublesSketch sketch = (ArrayOfDoublesSketch) agg.get();
    final double[][] entries = sketch.getValues();
    Assert.assertEquals(3, entries.length);
    final double[] testAExpected = {1, 2, 0};
    final double[] testBExpected = {4, 0, 3};
    final double[] testCExpected = {0, 5, 6};
    for (int i = 0; i < entries.length; i++) {
      final double[] entry = entries[i];
      Assert.assertEquals(3, entry.length);
      if (entry[0] == 1) {
        Assert.assertArrayEquals(testAExpected, entry, 0.001);
      } else if (entry[0] == 4) {
        Assert.assertArrayEquals(testBExpected, entry, 0.001);
      } else if (entry[0] == 0) {
        Assert.assertArrayEquals(testCExpected, entry, 0.001);
      } else {
        Assert.assertTrue(false);
      }
    }
  }

  private MetricFilterSpec buildMetricFilterSpec(String dimensionValue)
  {
    return new MetricFilterSpec(
        "value",
        new SelectorDimFilter("filterDim", dimensionValue, null, null)
    );
  }

  private ColumnSelectorFactory makeColumnSelector(
      final DimensionSelector sketchDimensionSelector,
      final DimensionSelector filterDimensionSelector,
      final ColumnValueSelector<?> valueSelector
  )
  {
    return new ColumnSelectorFactory()
    {

      @Override
      public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec)
      {
        final String dimension = dimensionSpec.getDimension();
        if ("sketchDim".equals(dimension)) {
          return dimensionSpec.decorate(sketchDimensionSelector);
        }
        if ("filterDim".equals(dimension)) {
          return dimensionSpec.decorate(filterDimensionSelector);
        }
        throw new IllegalArgumentException();
      }

      @Override
      public ColumnValueSelector<?> makeColumnValueSelector(String columnName)
      {
        if ("value".equals(columnName)) {
          return valueSelector;
        }
        throw new IllegalArgumentException();
      }

      @Override
      public ColumnCapabilities getColumnCapabilities(String columnName)
      {
        ColumnCapabilitiesImpl caps;
        if ("value".equals(columnName)) {
          caps = new ColumnCapabilitiesImpl();
          caps.setType(ValueType.DOUBLE);
          caps.setDictionaryEncoded(false);
          caps.setHasBitmapIndexes(false);
        } else {
          caps = new ColumnCapabilitiesImpl();
          caps.setType(ValueType.STRING);
          caps.setDictionaryEncoded(true);
          caps.setHasBitmapIndexes(true);
        }
        return caps;
      }
    };
  }

  private DimensionSelector buildBaseDimensionSelector(
      final String[] values,
      final TestFloatColumnSelector valueSelector
  )
  {
    final Map<String, Integer> valueToIndex = new HashMap<>();
    final Map<Integer, String> indexToValue = new HashMap<>();
    int index = 0;
    for (int i = 0; i < values.length; i++) {
      final String key = values[i];
      if (!valueToIndex.containsKey(key)) {
        valueToIndex.put(key, index);
        indexToValue.put(index, key);
        index++;
      }
    }

    return new AbstractDimensionSelector()
    {
      @Override
      public IndexedInts getRow()
      {
        SingleIndexedInt row = new SingleIndexedInt();
        int rowIndex = valueSelector.getIndex();
        final Integer valueIndex = valueToIndex.get(values[rowIndex]);
        if (valueIndex != null) {
          row.setValue(valueIndex);
        }
        return row;
      }

      @Override
      public ValueMatcher makeValueMatcher(String value)
      {
        return DimensionSelectorUtils.makeValueMatcherGeneric(this, value);
      }

      @Override
      public ValueMatcher makeValueMatcher(Predicate<String> predicate)
      {
        return DimensionSelectorUtils.makeValueMatcherGeneric(this, predicate);
      }

      @Override
      public int getValueCardinality()
      {
        return valueToIndex.size();
      }

      @Override
      public String lookupName(int id)
      {
        final String output = indexToValue.get(id);
        if (output == null) {
          throw new IllegalArgumentException();
        }
        return output;
      }

      @Override
      public boolean nameLookupPossibleInAdvance()
      {
        return true;
      }

      @Nullable
      @Override
      public IdLookup idLookup()
      {
        return new IdLookup()
        {
          @Override
          public int lookupId(String name)
          {
            final Integer output = valueToIndex.get(name);
            if (output == null) {
              throw new IllegalArgumentException();
            }
            return output;
          }
        };
      }

      @Override
      public Class classOfObject()
      {
        return Object.class;
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        // Don't care about runtime shape in tests
      }
    };
  }
}

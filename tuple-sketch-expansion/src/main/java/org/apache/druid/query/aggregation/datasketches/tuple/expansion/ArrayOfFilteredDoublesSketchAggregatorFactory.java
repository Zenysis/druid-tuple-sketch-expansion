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
import com.google.common.base.Preconditions;
import org.apache.datasketches.common.Util;
import org.apache.datasketches.tuple.arrayofdoubles.ArrayOfDoublesSetOperationBuilder;
import org.apache.datasketches.tuple.arrayofdoubles.ArrayOfDoublesSketch;
import org.apache.datasketches.tuple.arrayofdoubles.ArrayOfDoublesUnion;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.query.aggregation.AggregateCombiner;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.aggregation.datasketches.tuple.ArrayOfDoublesSketchBuildAggregator;
import org.apache.druid.query.aggregation.datasketches.tuple.ArrayOfDoublesSketchMergeAggregator;
import org.apache.druid.query.aggregation.datasketches.tuple.ArrayOfDoublesSketchOperations;
import org.apache.druid.query.aggregation.datasketches.tuple.NoopArrayOfDoublesSketchAggregator;
import org.apache.druid.query.aggregation.datasketches.tuple.NoopArrayOfDoublesSketchBufferAggregator;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.segment.BaseDoubleColumnValueSelector;
import org.apache.druid.segment.BaseObjectColumnValueSelector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.NilColumnValueSelector;
import org.apache.druid.segment.column.ValueType;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

public class ArrayOfFilteredDoublesSketchAggregatorFactory extends AggregatorFactory
{
  public static final byte CACHE_TYPE_ID = 0x75;

  public static final Comparator<ArrayOfDoublesSketch> COMPARATOR =
      Comparator.nullsFirst(Comparator.comparingDouble(ArrayOfDoublesSketch::getEstimate));

  private final String name;
  private final String fieldName;
  private final int nominalEntries;
  private final int numberOfValues;

  // If specified indicates building sketched from raw data, and also implies
  // the number of values.
  private final List<MetricFilterSpec> metricFilters;

  @JsonCreator
  public ArrayOfFilteredDoublesSketchAggregatorFactory(
      @JsonProperty("name") final String name,
      @JsonProperty("fieldName") final String fieldName,
      @JsonProperty("nominalEntries") @Nullable final Integer nominalEntries,
      @JsonProperty("metricFilters") final List<MetricFilterSpec> metricFilters,
      @JsonProperty("numberOfValues") @Nullable final Integer numberOfValues
  )
  {
    this.name = Preconditions.checkNotNull(
        name,
        "Must have a valid, non-null aggregator name"
    );
    this.fieldName = Preconditions.checkNotNull(
        fieldName,
        "Must have a valid, non-null fieldName"
    );
    this.nominalEntries = nominalEntries == null ? Common.DEFAULT_NOMINAL_ENTRIES : nominalEntries;
    Util.checkIfIntPowerOf2(this.nominalEntries, "nominalEntries");
    this.metricFilters = metricFilters;

    if (numberOfValues == null) {
      this.numberOfValues = metricFilters == null ? 1 : metricFilters.size();
    } else {
      this.numberOfValues = numberOfValues;
    }
    if (metricFilters != null && metricFilters.size() != this.numberOfValues) {
      throw new IAE(
          "Number of metricFilters [%d] must agree with numValues [%d]",
          metricFilters.size(),
          this.numberOfValues
      );
    }
  }

  @Override
  public Aggregator factorize(final ColumnSelectorFactory metricFactory)
  {

    if (metricFilters == null) { // input is sketches, use merge aggregator
      final BaseObjectColumnValueSelector<ArrayOfDoublesSketch> selector = metricFactory
          .makeColumnValueSelector(fieldName);
      if (selector instanceof NilColumnValueSelector) {
        return new NoopArrayOfDoublesSketchAggregator(numberOfValues);
      }
      return new ArrayOfDoublesSketchMergeAggregator(selector, nominalEntries, numberOfValues);
    }

    // input is raw data (key and array of values), use build aggregator
    final DimensionSelector keySelector = metricFactory
        .makeDimensionSelector(new DefaultDimensionSpec(fieldName, fieldName));
    if (DimensionSelector.isNilSelector(keySelector)) {
      return new NoopArrayOfDoublesSketchAggregator(numberOfValues);
    }

    final List<BaseDoubleColumnValueSelector> selectors = new ArrayList<>();
    for (final MetricFilterSpec metricFilter : metricFilters) {
      selectors.add(metricFilter.buildSelector(metricFactory));
    }
    return new ArrayOfDoublesSketchBuildAggregator(keySelector, selectors, nominalEntries);
  }

  @Override
  public BufferAggregator factorizeBuffered(final ColumnSelectorFactory metricFactory)
  {
    if (metricFilters == null) { // input is sketches, use merge aggregator
      final BaseObjectColumnValueSelector<ArrayOfDoublesSketch> selector = metricFactory
          .makeColumnValueSelector(fieldName);
      if (selector instanceof NilColumnValueSelector) {
        return new NoopArrayOfDoublesSketchBufferAggregator(numberOfValues);
      }
      return new ArrayOfFilteredDoublesSketchMergeBufferAggregator(
          selector,
          nominalEntries,
          numberOfValues,
          getMaxIntermediateSizeWithNulls()
      );
    }

    // input is raw data (key and array of values), use build aggregator
    final DimensionSelector keySelector = metricFactory
        .makeDimensionSelector(new DefaultDimensionSpec(fieldName, fieldName));
    if (DimensionSelector.isNilSelector(keySelector)) {
      return new NoopArrayOfDoublesSketchBufferAggregator(numberOfValues);
    }

    final List<BaseDoubleColumnValueSelector> selectors = new ArrayList<>();
    for (final MetricFilterSpec metricFilter : metricFilters) {
      selectors.add(metricFilter.buildSelector(metricFactory));
    }
    return new ArrayOfFilteredDoublesSketchBuildBufferAggregator(
        keySelector,
        selectors,
        nominalEntries,
        getMaxIntermediateSizeWithNulls()
    );
  }

  @Override
  public Object deserialize(final Object object)
  {
    return ArrayOfDoublesSketchOperations.deserialize(object);
  }

  @Override
  public Comparator<ArrayOfDoublesSketch> getComparator()
  {
    return COMPARATOR;
  }

  @Override
  public Object combine(@Nullable final Object lhs, @Nullable final Object rhs)
  {
    // NOTE(stephen): Union default operation when key exists in both sides is
    // to sum the tuples. This is what we want.
    final ArrayOfDoublesUnion union = new ArrayOfDoublesSetOperationBuilder()
        .setNominalEntries(nominalEntries)
        .setNumberOfValues(numberOfValues)
        .buildUnion();
    if (lhs != null) {
      union.union((ArrayOfDoublesSketch) lhs);
    }
    if (rhs != null) {
      union.union((ArrayOfDoublesSketch) rhs);
    }
    return union.getResult();
  }

  @Override
  public AggregateCombiner makeAggregateCombiner()
  {
    throw new UOE("ArrayOfFilteredDoublesSketchAggregatorFactory is not supported during ingestion for rollup");
  }

  @Override
  @JsonProperty
  public String getName()
  {
    return name;
  }

  @JsonProperty
  public String getFieldName()
  {
    return fieldName;
  }

  @JsonProperty
  public int getNominalEntries()
  {
    return nominalEntries;
  }

  @JsonProperty
  public List<MetricFilterSpec> getMetricFilters()
  {
    return metricFilters;
  }

  @JsonProperty
  public int getNumberOfValues()
  {
    return numberOfValues;
  }

  @Override
  public List<String> requiredFields()
  {
    return Collections.singletonList(fieldName);
  }

  @Override
  public byte[] getCacheKey()
  {
    final CacheKeyBuilder builder = new CacheKeyBuilder(CACHE_TYPE_ID)
        .appendString(name)
        .appendString(fieldName)
        .appendInt(nominalEntries)
        .appendInt(numberOfValues);
    if (metricFilters != null) {
      for (final MetricFilterSpec metricFilter : metricFilters) {
        builder.appendByteArray(metricFilter.getCacheKey());
      }
    }
    return builder.build();
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return ArrayOfDoublesUnion.getMaxBytes(nominalEntries, numberOfValues);
  }

  @Override
  public List<AggregatorFactory> getRequiredColumns()
  {
    return Collections.singletonList(
      new ArrayOfFilteredDoublesSketchAggregatorFactory(
          fieldName,
          fieldName,
          nominalEntries,
          metricFilters,
          numberOfValues
      )
    );
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new ArrayOfFilteredDoublesSketchAggregatorFactory(
        name,
        name,
        nominalEntries,
        null,
        numberOfValues
    );
  }

  @Nullable
  @Override
  public Object finalizeComputation(@Nullable final Object object)
  {
    return object == null ? null : ((ArrayOfDoublesSketch) object).getEstimate();
  }

  @Override
  public String getComplexTypeName()
  {
    if (metricFilters == null) {
      return TupleSketchExpansionModule.ARRAY_OF_FILTERED_DOUBLES_SKETCH_MERGE_AGG;
    }
    return TupleSketchExpansionModule.ARRAY_OF_FILTERED_DOUBLES_SKETCH_BUILD_AGG;
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
  public ValueType getFinalizedType()
  {
    return ValueType.DOUBLE;
  }

  @Override
  public boolean equals(final Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ArrayOfFilteredDoublesSketchAggregatorFactory)) {
      return false;
    }
    final ArrayOfFilteredDoublesSketchAggregatorFactory that = (ArrayOfFilteredDoublesSketchAggregatorFactory) o;
    if (!name.equals(that.name)) {
      return false;
    }
    if (!fieldName.equals(that.fieldName)) {
      return false;
    }
    if (nominalEntries != that.nominalEntries) {
      return false;
    }
    if (!Objects.equals(metricFilters, that.metricFilters)) {
      return false;
    }
    return numberOfValues == that.numberOfValues;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(name, fieldName, nominalEntries, metricFilters, numberOfValues);
  }

  @Override
  public String toString()
  {
    return this.getClass().getSimpleName() + "{"
        + "fieldName=" + fieldName
        + ", name=" + name
        + ", nominalEntries=" + nominalEntries
        + ", metricFilters=" + metricFilters
        + ", numberOfValues=" + numberOfValues
        + "}";
  }
}

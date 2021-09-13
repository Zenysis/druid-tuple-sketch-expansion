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

import org.apache.datasketches.tuple.ArrayOfDoublesUpdatableSketch;
import org.apache.datasketches.tuple.ArrayOfDoublesUpdatableSketchBuilder;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.BaseDoubleColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.data.IndexedInts;

import javax.annotation.Nullable;

import java.nio.ByteBuffer;
import java.util.List;

public class ArrayOfFilteredDoublesSketchBuildBufferAggregator implements BufferAggregator
{
  private final DimensionSelector keySelector;
  private final BaseDoubleColumnValueSelector[] valueSelectors;
  private final int nominalEntries;
  private final int numberOfValues;
  private final int maxIntermediateSize;
  @Nullable
  private double[] values; // not part of the state, but to reuse in aggregate() method

  public ArrayOfFilteredDoublesSketchBuildBufferAggregator(
      final DimensionSelector keySelector,
      final List<BaseDoubleColumnValueSelector> valueSelectors,
      int nominalEntries,
      int maxIntermediateSize
  )
  {
    this.keySelector = keySelector;
    this.valueSelectors = valueSelectors.toArray(new BaseDoubleColumnValueSelector[0]);
    this.nominalEntries = nominalEntries;

    final int numberOfValues = valueSelectors.size();
    this.numberOfValues = numberOfValues;
    this.maxIntermediateSize = maxIntermediateSize;
    values = new double[numberOfValues];
  }

  @Override
  public void init(final ByteBuffer buf, final int position)
  {
    new ArrayOfDoublesUpdatableSketchBuilder()
        .setNominalEntries(nominalEntries)
        .setNumberOfValues(numberOfValues)
        .build(ArrayOfDoublesBufferHelper.getWritableMemory(buf, position, maxIntermediateSize));
  }

  @Override
  public void aggregate(final ByteBuffer buf, final int position)
  {
    for (int i = 0; i < valueSelectors.length; i++) {
      if (valueSelectors[i].isNull()) {
        return;
      } else {
        values[i] = valueSelectors[i].getDouble();
      }
    }
    final IndexedInts keys = keySelector.getRow();
    final ArrayOfDoublesUpdatableSketch sketch = ArrayOfDoublesBufferHelper.getUpdatableSketchAtPosition(
        buf,
        position,
        numberOfValues,
        maxIntermediateSize
    );
    for (int i = 0, keysSize = keys.size(); i < keysSize; i++) {
      final String key = keySelector.lookupName(keys.get(i));
      sketch.update(key, values);
    }
  }

  /**
   * The returned sketch is a separate instance of ArrayOfDoublesCompactSketch
   * representing the current state of the aggregation, and is not affected by consequent
   * aggregate() calls
   */
  @Override
  public Object get(final ByteBuffer buf, final int position)
  {
    return ArrayOfDoublesBufferHelper.getUpdatableSketchAtPosition(
        buf,
        position,
        numberOfValues,
        maxIntermediateSize
    ).compact();
  }

  @Override
  public float getFloat(final ByteBuffer buf, final int position)
  {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public long getLong(final ByteBuffer buf, final int position)
  {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void close()
  {
    values = null;
  }

  @Override
  public void inspectRuntimeShape(final RuntimeShapeInspector inspector)
  {
    inspector.visit("keySelector", keySelector);
    inspector.visit("valueSelectors", valueSelectors);
  }
}

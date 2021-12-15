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

import org.apache.datasketches.tuple.arrayofdoubles.ArrayOfDoublesSetOperationBuilder;
import org.apache.datasketches.tuple.arrayofdoubles.ArrayOfDoublesSketch;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.BaseObjectColumnValueSelector;

import java.nio.ByteBuffer;

/**
 * This aggregator merges existing sketches.
 * The input column contains ArrayOfDoublesSketch.
 * The output is {@link ArrayOfDoublesSketch} that is a union of the input sketches.
 */
public class ArrayOfFilteredDoublesSketchMergeBufferAggregator implements BufferAggregator
{
  private final BaseObjectColumnValueSelector<ArrayOfDoublesSketch> selector;
  private final int nominalEntries;
  private final int numberOfValues;
  private final int maxIntermediateSize;

  public ArrayOfFilteredDoublesSketchMergeBufferAggregator(
      final BaseObjectColumnValueSelector<ArrayOfDoublesSketch> selector,
      final int nominalEntries,
      final int numberOfValues,
      final int maxIntermediateSize
  )
  {
    this.selector = selector;
    this.nominalEntries = nominalEntries;
    this.numberOfValues = numberOfValues;
    this.maxIntermediateSize = maxIntermediateSize;
  }

  @Override
  public void init(final ByteBuffer buf, final int position)
  {
    new ArrayOfDoublesSetOperationBuilder()
        .setNominalEntries(nominalEntries)
        .setNumberOfValues(numberOfValues)
        .buildUnion(ArrayOfDoublesBufferHelper.getWritableMemory(buf, position, maxIntermediateSize));
  }

  @Override
  public void aggregate(final ByteBuffer buf, final int position)
  {
    final ArrayOfDoublesSketch update = selector.getObject();
    if (update == null) {
      return;
    }

    ArrayOfDoublesBufferHelper.getUnionSketchAtPosition(
        buf,
        position,
        numberOfValues,
        maxIntermediateSize
    ).update(update);
  }

  /**
   * The returned sketch is a separate instance of ArrayOfDoublesCompactSketch
   * representing the current state of the aggregation, and is not affected by
   * consequent aggregate() calls.
   */
  @Override
  public Object get(final ByteBuffer buf, final int position)
  {
    return ArrayOfDoublesBufferHelper.getUnionSketchAtPosition(
        buf,
        position,
        numberOfValues,
        maxIntermediateSize
    ).getResult();
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
  }

  @Override
  public void inspectRuntimeShape(final RuntimeShapeInspector inspector)
  {
    inspector.visit("selector", selector);
  }
}

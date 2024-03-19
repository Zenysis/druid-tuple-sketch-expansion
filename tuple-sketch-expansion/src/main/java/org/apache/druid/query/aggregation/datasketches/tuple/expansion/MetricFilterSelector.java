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

import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.BaseDoubleColumnValueSelector;

import java.util.Objects;

public class MetricFilterSelector implements BaseDoubleColumnValueSelector
{
  private final BaseDoubleColumnValueSelector valueSelector;
  private final ValueMatcher valueMatcher;

  public MetricFilterSelector(
      BaseDoubleColumnValueSelector valueSelector,
      ValueMatcher valueMatcher
  )
  {
    this.valueSelector = valueSelector;
    this.valueMatcher = valueMatcher;
  }

  public boolean matches()
  {
    return valueMatcher.matches(true);
  }

  @Override
  public double getDouble()
  {
    return valueMatcher.matches(true) ? valueSelector.getDouble() : 0d;
  }

  @Override
  public boolean isNull()
  {
    return valueSelector.isNull();
  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
    inspector.visit("valueSelector", valueSelector);
    inspector.visit("valueMatcher", valueMatcher);
  }

  @Override
  public boolean equals(final Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final MetricFilterSelector that = (MetricFilterSelector) o;
    return Objects.equals(valueSelector, that.valueSelector) &&
           Objects.equals(valueMatcher, that.valueMatcher);
  }

  @Override
  public String toString()
  {
    return "MetricFilterSelector{" +
           "valueSelector='" + valueSelector + '\'' +
           ", valueMatcher='" + valueMatcher + '\'' +
           '}';
  }
}

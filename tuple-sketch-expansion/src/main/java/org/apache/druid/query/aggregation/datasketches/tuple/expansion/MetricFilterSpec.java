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
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.segment.ColumnSelectorFactory;

import java.util.Objects;

public class MetricFilterSpec
{
  public static final byte CACHE_TYPE_ID = 0x72;

  private final String metricColumn;
  private final DimFilter dimFilter;
  private final Filter filter;

  @JsonCreator
  public MetricFilterSpec(
      @JsonProperty("metricColumn") final String metricColumn,
      @JsonProperty("filter") final DimFilter dimFilter
  )
  {
    this.metricColumn = Preconditions.checkNotNull(
        metricColumn,
        "Missing 'metricColumn' in field spec"
    );
    this.dimFilter = Preconditions.checkNotNull(
        dimFilter,
        "Missing 'filter' in field spec"
    );
    this.filter = dimFilter.toFilter();
  }

  public MetricFilterSelector buildSelector(final ColumnSelectorFactory columnSelectorFactory)
  {
    return new MetricFilterSelector(
        columnSelectorFactory.makeColumnValueSelector(metricColumn),
        filter.makeMatcher(columnSelectorFactory)
    );
  }

  @JsonProperty
  public String getMetricColumn()
  {
    return metricColumn;
  }

  @JsonProperty
  public DimFilter getFilter()
  {
    return dimFilter;
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
    final MetricFilterSpec that = (MetricFilterSpec) o;
    return Objects.equals(metricColumn, that.metricColumn) &&
           Objects.equals(filter, that.filter);
  }

  public byte[] getCacheKey()
  {
    return new CacheKeyBuilder(CACHE_TYPE_ID)
        .appendByteArray(dimFilter.getCacheKey())
        .appendString(metricColumn)
        .build();
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(metricColumn, filter);
  }

  @Override
  public String toString()
  {
    return "MetricFilterSpec{" +
           "metricColumn='" + metricColumn + '\'' +
           ", filter='" + filter + '\'' +
           '}';
  }
}

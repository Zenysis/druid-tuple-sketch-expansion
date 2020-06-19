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

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.inject.Binder;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.query.aggregation.datasketches.RawInputValueExtractor;
import org.apache.druid.segment.serde.ComplexMetricExtractor;
import org.apache.druid.segment.serde.ComplexMetrics;

import java.util.Collections;
import java.util.List;

public class TupleSketchExpansionModule implements DruidModule
{

  public static final String ARRAY_OF_FILTERED_DOUBLES_SKETCH = "arrayOfFilteredDoublesSketch";

  public static final String ARRAY_OF_FILTERED_DOUBLES_SKETCH_MERGE_AGG = "arrayOfFilteredDoublesSketchMerge";
  public static final String ARRAY_OF_FILTERED_DOUBLES_SKETCH_BUILD_AGG = "arrayOfFilteredDoublesSketchBuild";

  @Override
  public void configure(final Binder binder)
  {
    ComplexMetrics.registerSerde(
        ARRAY_OF_FILTERED_DOUBLES_SKETCH,
        new ArrayOfFilteredDoublesSketchMergeComplexMetricSerde()
    );
    ComplexMetrics.registerSerde(
        ARRAY_OF_FILTERED_DOUBLES_SKETCH_MERGE_AGG,
        new ArrayOfFilteredDoublesSketchMergeComplexMetricSerde()
    );
    ComplexMetrics.registerSerde(
        ARRAY_OF_FILTERED_DOUBLES_SKETCH_BUILD_AGG,
        new ArrayOfFilteredDoublesSketchMergeComplexMetricSerde()
        {
          @Override
          public ComplexMetricExtractor getExtractor()
          {
            return RawInputValueExtractor.getInstance();
          }
        }
    );
  }

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return Collections.<Module>singletonList(
        new SimpleModule("TupleSketchExpansionModule").registerSubtypes(
            new NamedType(
                ArrayOfFilteredDoublesSketchAggregatorFactory.class,
                ARRAY_OF_FILTERED_DOUBLES_SKETCH
            ),
            new NamedType(
                ArrayOfDoublesCollapseExpressionPostAggregator.class,
                "arrayOfDoublesCollapseExpression"
            ),
            new NamedType(
                ArrayOfDoublesFilterExpressionPostAggregator.class,
                "arrayOfDoublesFilterExpression"
            )
        )
    );
  }
}

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

import org.apache.datasketches.memory.WritableMemory;
import org.apache.datasketches.tuple.arrayofdoubles.ArrayOfDoublesSketches;
import org.apache.datasketches.tuple.arrayofdoubles.ArrayOfDoublesUnion;
import org.apache.datasketches.tuple.arrayofdoubles.ArrayOfDoublesUpdatableSketch;
import org.apache.druid.java.util.common.logger.Logger;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class ArrayOfDoublesBufferHelper
{
  private static final Logger log = new Logger(ArrayOfDoublesBufferHelper.class);

  /**
   * Retrieves an updatable sketch at a particular position.
   */
  public static ArrayOfDoublesUpdatableSketch getUpdatableSketchAtPosition(
      final ByteBuffer buffer,
      final int position,
      final int numberOfValues,
      final int maxIntermediateSize
  )
  {
    final WritableMemory mem = getWritableMemory(buffer, position, maxIntermediateSize);
    patchCorruptedMemoryIfNeeded(mem, numberOfValues);
    return ArrayOfDoublesSketches.wrapUpdatableSketch(mem);
  }

  /**
   * Retrieves a union sketch at a particular position.
   */
  public static ArrayOfDoublesUnion getUnionSketchAtPosition(
      final ByteBuffer buffer,
      final int position,
      final int numberOfValues,
      final int maxIntermediateSize
  )
  {
    final WritableMemory mem = getWritableMemory(buffer, position, maxIntermediateSize);
    patchCorruptedMemoryIfNeeded(mem, numberOfValues);
    return ArrayOfDoublesSketches.wrapUnion(mem);
  }

  /**
   * Retrieves a WritableMemory instance over the provided ByteBuffer.
   */
  public static WritableMemory getWritableMemory(
      final ByteBuffer buffer,
      final int position,
      final int maxIntermediateSize
  )
  {
    return WritableMemory
        .wrap(buffer, ByteOrder.LITTLE_ENDIAN)
        .writableRegion(position, maxIntermediateSize);
  }

  /**
   * For very large queries, there is the possibility that a sketch cannot be
   * cleanly "wrapped" from the ByteBuffer Druid gives to us. This seems to
   * happen for large queries (> 16k rows) and after Druid calls `relocate` at
   * least once. In our review of these failures, the memory does not actually
   * appear to be corrupted, only the specific values that Datasketches needs
   * to check are incorrect. We have no idea why this happens, and it is very
   * difficult to come up with a clean test case. For our uses, it's important
   * that the sketch aggregation never fail, so we patch out any corruption in
   * the memory and ensure that Datasketches can properly wrap the memory. In
   * experimental reviews, the values produced absent any corruption versus
   * after the memory is patched are the same and there is no issue seen.
   *
   * The corruption message raised by Datasketches is:
   * Possible corruption: Invalid PreambleLongs value for family TUPLE:
   */
  private static void patchCorruptedMemoryIfNeeded(
      final WritableMemory memory,
      final int numberOfValues
  )
  {
    // Preamble Longs and Serial Version can get corrupted somehow.
    final byte preambleLongs = memory.getByte(0);
    final byte serialVersion = memory.getByte(1);

    // If both values are correct, then no corruption happened.
    if (preambleLongs == 1 && serialVersion == 1) {
      return;
    }

    // Try to recover from the corruption.
    // NOTE(stephen): I am not sure what the downstream effects will be of this.
    // It could be possible a corrupt sketch is corrupt in multiple places? We
    // just have no idea why corruption has been happening and we wanted to
    // reduce those errors if they don't actually matter.

    // These values should not be corrupted.
    final byte familyId = memory.getByte(2);
    final byte sketchType = memory.getByte(3);
    final byte detectedNumberOfValues = memory.getByte(5);
    if (
        familyId != 9 ||
        sketchType != 2 ||
        detectedNumberOfValues != numberOfValues
    ) {
      log.warn("Unable to recover corrupted sketch");
      log.warn("Expected Family ID: 9\tActual FamilyID: " + familyId);
      log.warn("Expected Sketch Type: 2\tActual Sketch Type: " + sketchType);
      log.warn("Expected Number of Values: " + numberOfValues + "\tActual Number of Values: " + detectedNumberOfValues);
      return;
    }

    // Set the preambleLongs and serial version to the expected value after
    // relocation.
    memory.putByte(0, (byte) 1);
    memory.putByte(1, (byte) 1);
  }
}

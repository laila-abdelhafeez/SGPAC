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
package org.apache.spark.sql.sedona_viz.expressions

import org.apache.sedona.viz.core.Serde.PixelSerializer
import org.apache.sedona.viz.core.VisualizationPartitioner
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.{DataType, StringType}
import org.apache.spark.unsafe.types.UTF8String

case class ST_TileName(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback {
  assert(inputExpressions.length == 2)
  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    val inputArray = inputExpressions(0).eval(input).asInstanceOf[ArrayData]
    val zoomLevel = inputExpressions(1).eval(input).asInstanceOf[Int]
    val partPerAxis = Math.pow(2, zoomLevel).intValue()
    val serializer = new PixelSerializer
    val pixel = serializer.readPixel(inputArray.toByteArray())
    val id = VisualizationPartitioner.Calculate2DPartitionId(pixel.getResolutionX, pixel.getResolutionY, partPerAxis, partPerAxis, pixel.getX.intValue(), pixel.getY.intValue())
    UTF8String.fromString(zoomLevel + "-" + id._1 + "-" + id._2)
  }

  override def dataType: DataType = StringType

  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}
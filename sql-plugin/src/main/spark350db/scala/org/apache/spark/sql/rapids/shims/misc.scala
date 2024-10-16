/*
 * Copyright (c) 2024, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*** spark-rapids-shim-json-lines
{"spark": "350db"}
{"spark": "400"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids.shims

import com.nvidia.spark.rapids.{GpuColumnVector, GpuExpression}
import com.nvidia.spark.rapids.shims.ShimBinaryExpression 

import org.apache.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes}
import org.apache.spark.sql.errors.QueryExecutionErrors.raiseError
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch

case class GpuRaiseError(errorClass: Expression, errorParams: Expression) 
   extends ShimBinaryExpression with ImplicitCastInputTypes with GpuExpression {

  override def prettyName: String = "raise_error"

  override def left: Expression = errorClass

  override def right: Expression = errorParams

  override def dataType: DataType = NullType

  override def inputTypes: Seq[AbstractDataType] =
    Seq(StringType, MapType(StringType, StringType))

  /** Could evaluating this expression cause side-effects, such as throwing an exception? */
  override def hasSideEffects: Boolean = true

  override def columnarEval(batch: ColumnarBatch): GpuColumnVector = {
    // Take the first one as the error message
    val iterator = batch.rowIterator()
    if (iterator.hasNext()) {
      val row = iterator.next()
      if (batch.numCols > 2) {
          throw new IllegalStateException(s"Expected only two children but found ${batch.numCols}")
      }
      val error = row.getUTF8String(0)
      val params = row.getMap(1)
      throw raiseError(error, params)
    } else {
      throw new IllegalStateException("Empty batch found in GpuRaiseError")
    }
  }
}

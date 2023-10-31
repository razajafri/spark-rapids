/*
 * Copyright (c) 2023, NVIDIA CORPORATION.
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
{"spark": "341db"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids.execution.python.shims

import java.io.DataOutputStream

import ai.rapids.cudf.{ArrowIPCWriterOptions, NvtxColor, NvtxRange, Table}
import com.nvidia.spark.rapids.{GpuColumnVector, GpuSemaphore}
import com.nvidia.spark.rapids.Arm.withResource

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.api.python.{ChainedPythonFunctions, PythonRDD, PythonWorker}
import org.apache.spark.sql.execution.python.PythonUDFRunner
import org.apache.spark.sql.rapids.execution.python.{BufferToStreamWriter, GpuArrowPythonRunner}
import org.apache.spark.sql.rapids.execution.python.GpuPythonRunnerBase
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.Utils

/**
 * Python UDF Runner for cogrouped UDFs, designed for `GpuFlatMapCoGroupsInPandasExec` only.
 *
 * It sends Arrow batches from two different DataFrames, groups them in Python,
 * and receive it back in JVM as batches of single DataFrame.
 */
class GpuCoGroupedArrowPythonRunner(
                                     funcs: Seq[ChainedPythonFunctions],
                                     evalType: Int,
                                     argOffsets: Array[Array[Int]],
                                     leftSchema: StructType,
                                     rightSchema: StructType,
                                     timeZoneId: String,
                                     conf: Map[String, String],
                                     batchSize: Int,
                                     pythonOutSchema: StructType)
  extends GpuPythonRunnerBase[(ColumnarBatch, ColumnarBatch)](funcs, evalType, argOffsets)
    with GpuPythonArrowOutput {

  protected override def newWriter(
                                    env: SparkEnv,
                                    worker: PythonWorker,
                                    inputIterator: Iterator[(ColumnarBatch, ColumnarBatch)],
                                    partitionIndex: Int,
                                    context: TaskContext): Writer = {
    new Writer(env, worker, inputIterator, partitionIndex, context) {

      protected override def writeCommand(dataOut: DataOutputStream): Unit = {

        // Write config for the worker as a number of key -> value pairs of strings
        dataOut.writeInt(conf.size)
        for ((k, v) <- conf) {
          PythonRDD.writeUTF(k, dataOut)
          PythonRDD.writeUTF(v, dataOut)
        }

        PythonUDFRunner.writeUDFs(dataOut, funcs, argOffsets)
      }

      override def writeNextInputToStream(dataOut: DataOutputStream): Boolean = {
        // For each we first send the number of dataframes in each group then send
        // first df, then send second df.  End of data is marked by sending 0.
        var wrote = false
        while (inputIterator.hasNext) {
          wrote = false
          dataOut.writeInt(2)
          val (leftGroupBatch, rightGroupBatch) = inputIterator.next()
          withResource(Seq(leftGroupBatch, rightGroupBatch)) { _ =>
            wrote = writeGroupBatch(leftGroupBatch, leftSchema, dataOut)
            wrote = writeGroupBatch(rightGroupBatch, rightSchema, dataOut)
          }
        }
        // The iterator can grab the semaphore even on an empty batch
        GpuSemaphore.releaseIfNecessary(TaskContext.get())
        dataOut.writeInt(0)
        wrote
      }

      private def writeGroupBatch(groupBatch: ColumnarBatch, batchSchema: StructType,
                                  dataOut: DataOutputStream): Boolean = {
        val writer = {
          val builder = ArrowIPCWriterOptions.builder()
          builder.withMaxChunkSize(batchSize)
          builder.withCallback((table: Table) => {
            table.close()
            GpuSemaphore.releaseIfNecessary(TaskContext.get())
          })
          // Flatten the names of nested struct columns, required by cudf arrow IPC writer.
          GpuArrowPythonRunner.flattenNames(batchSchema).foreach { case (name, nullable) =>
            if (nullable) {
              builder.withColumnNames(name)
            } else {
              builder.withNotNullableColumnNames(name)
            }
          }
          Table.writeArrowIPCChunked(builder.build(), new BufferToStreamWriter(dataOut))
        }
        var wrote = false
        Utils.tryWithSafeFinally {
          withResource(new NvtxRange("write python batch", NvtxColor.DARK_GREEN)) { _ =>
            // The callback will handle closing table and releasing the semaphore
            writer.write(GpuColumnVector.from(groupBatch))
            wrote = true
          }
        } {
          writer.close()
          dataOut.flush()
        }
        wrote
      } // end of writeGroup
    }
  } // end of newWriterThread

  def toBatch(table: Table): ColumnarBatch = {
    GpuColumnVector.from(table, GpuColumnVector.extractTypes(pythonOutSchema))
  }
}

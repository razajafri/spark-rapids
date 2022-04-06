/*
 * Copyright (c) 2020-2022, NVIDIA CORPORATION.
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

package org.apache.spark.sql.execution.columnar.rapids

import com.nvidia.spark.rapids.Data

import org.apache.spark.TaskContext
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.columnar.{CachedBatch, SimpleMetricsCachedBatchSerializer}
import org.apache.spark.sql.execution.columnar.{ColumnAccessor, ColumnBuilder, DefaultCachedBatch, GenerateColumnAccessor}
import org.apache.spark.sql.execution.vectorized.{OffHeapColumnVector, OnHeapColumnVector, WritableColumnVector}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{BooleanType, ByteType, DoubleType, FloatType, IntegerType, LongType, ShortType, StructType, UserDefinedType}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.storage.StorageLevel

/**
 * The default implementation of CachedBatchSerializer.
 */
class DefaultCachedBatchSerializer extends SimpleMetricsCachedBatchSerializer {
  override def supportsColumnarInput(schema: Seq[Attribute]): Boolean = false

  override def convertColumnarBatchToCachedBatch(
      input: RDD[ColumnarBatch],
      schema: Seq[Attribute],
      storageLevel: StorageLevel,
      conf: SQLConf): RDD[CachedBatch] =
    throw new IllegalStateException("Columnar input is not supported")

  override def convertInternalRowToCachedBatch(
      input: RDD[InternalRow],
      schema: Seq[Attribute],
      storageLevel: StorageLevel,
      conf: SQLConf): RDD[CachedBatch] = {
    val batchSize = conf.columnBatchSize
    val useCompression = conf.useCompression
    convertForCacheInternal(input, schema, batchSize, useCompression)
  }

  def convertForCacheInternal(
      input: RDD[InternalRow],
      output: Seq[Attribute],
      batchSize: Int,
      useCompression: Boolean): RDD[CachedBatch] = {
    input.mapPartitionsInternal { rowIterator =>
      TaskContext.get().addTaskCompletionListener(Data)
      new Iterator[DefaultCachedBatch] {
        def next(): DefaultCachedBatch = {
          val start = System.currentTimeMillis()
          val columnBuilders = output.map { attribute =>
            ColumnBuilder(attribute.dataType, batchSize, attribute.name, useCompression)
          }.toArray

          var rowCount = 0
          var totalSize = 0L
          while (rowIterator.hasNext && rowCount < batchSize
              && totalSize < ColumnBuilder.MAX_BATCH_SIZE_IN_BYTE) {
            val row = rowIterator.next()

            // Added for SPARK-6082. This assertion can be useful for scenarios when something
            // like Hive TRANSFORM is used. The external data generation script used in TRANSFORM
            // may result malformed rows, causing ArrayIndexOutOfBoundsException, which is somewhat
            // hard to decipher.
            assert(
              row.numFields == columnBuilders.length,
              s"Row column number mismatch, expected ${output.size} columns, " +
                  s"but got ${row.numFields}." +
                  s"\nRow content: $row")

            var i = 0
            totalSize = 0
            while (i < row.numFields) {
              columnBuilders(i).appendFrom(row, i)
              totalSize += columnBuilders(i).columnStats.sizeInBytes
              i += 1
            }
            rowCount += 1
          }

          val stats = InternalRow.fromSeq(
            columnBuilders.flatMap(_.columnStats.collectedStatistics).toSeq)
          val b = DefaultCachedBatch(rowCount, columnBuilders.map { builder =>
            JavaUtils.bufferToArray(builder.build())
          }, stats)
          val end = System.currentTimeMillis()
          val d = new Data("write", end - start, "DEFA", "?")
          Data.add(d)
          b
        }

        def hasNext: Boolean = rowIterator.hasNext
      }
    }
  }

  override def supportsColumnarOutput(schema: StructType): Boolean = schema.fields.forall(f =>
    f.dataType match {
      // More types can be supported, but this is to match the original implementation that
      // only supported primitive types "for ease of review"
      case BooleanType | ByteType | ShortType | IntegerType | LongType |
           FloatType | DoubleType => true
      case _ => false
    })

  override def vectorTypes(attributes: Seq[Attribute], conf: SQLConf): Option[Seq[String]] =
    Option(Seq.fill(attributes.length)(
      if (!conf.offHeapColumnVectorEnabled) {
        classOf[OnHeapColumnVector].getName
      } else {
        classOf[OffHeapColumnVector].getName
      }
    ))

  override def convertCachedBatchToColumnarBatch(
      input: RDD[CachedBatch],
      cacheAttributes: Seq[Attribute],
      selectedAttributes: Seq[Attribute],
      conf: SQLConf): RDD[ColumnarBatch] = {
    val offHeapColumnVectorEnabled = conf.offHeapColumnVectorEnabled
    val outputSchema = StructType.fromAttributes(selectedAttributes)
    val columnIndices =
      selectedAttributes.map(a => cacheAttributes.map(o => o.exprId).indexOf(a.exprId)).toArray

    def createAndDecompressColumn(cb: CachedBatch): ColumnarBatch = {
      val cachedColumnarBatch = cb.asInstanceOf[DefaultCachedBatch]
      val rowCount = cachedColumnarBatch.numRows
      val taskContext = Option(TaskContext.get())
      val columnVectors = if (!offHeapColumnVectorEnabled || taskContext.isEmpty) {
        OnHeapColumnVector.allocateColumns(rowCount, outputSchema)
      } else {
        OffHeapColumnVector.allocateColumns(rowCount, outputSchema)
      }
      val columnarBatch = new ColumnarBatch(columnVectors.asInstanceOf[Array[ColumnVector]])
      columnarBatch.setNumRows(rowCount)

      for (i <- selectedAttributes.indices) {
        ColumnAccessor.decompress(
          cachedColumnarBatch.buffers(columnIndices(i)),
          columnarBatch.column(i).asInstanceOf[WritableColumnVector],
          outputSchema.fields(i).dataType, rowCount)
      }
      taskContext.foreach(_.addTaskCompletionListener[Unit](_ => columnarBatch.close()))
      columnarBatch
    }

    input.map(b => {
      TaskContext.get().addTaskCompletionListener(Data)
      val start = System.currentTimeMillis()
      val cb = createAndDecompressColumn(b)
      val end = System.currentTimeMillis()
      val d = new Data("read", end - start, "DEFA", "?")
      Data.add(d)
      cb
    })
  }

  override def convertCachedBatchToInternalRow(
      input: RDD[CachedBatch],
      cacheAttributes: Seq[Attribute],
      selectedAttributes: Seq[Attribute],
      conf: SQLConf): RDD[InternalRow] = {
    // Find the ordinals and data types of the requested columns.
    val (requestedColumnIndices, requestedColumnDataTypes) =
      selectedAttributes.map { a =>
        cacheAttributes.map(_.exprId).indexOf(a.exprId) -> a.dataType
      }.unzip

    val columnTypes = requestedColumnDataTypes.map {
      case udt: UserDefinedType[_] => udt.sqlType
      case other => other
    }.toArray

    input.mapPartitionsInternal { cachedBatchIterator =>
      TaskContext.get().addTaskCompletionListener(Data)
      val columnarIterator = GenerateColumnAccessor.generate(columnTypes)
      columnarIterator.initialize(cachedBatchIterator.asInstanceOf[Iterator[DefaultCachedBatch]],
        columnTypes,
        requestedColumnIndices.toArray)
      new MyIter(columnarIterator)
    }
  }
}

class MyIter(iter: Iterator[InternalRow]) extends Iterator[InternalRow] with Serializable {

  override def hasNext: Boolean = iter.hasNext

  override def next(): InternalRow = {
    val start = System.currentTimeMillis()
    val a = iter.next()
    val end = System.currentTimeMillis()
    Data.add(new Data("read", end - start, "DEFA", "?"))
    a
  }
}

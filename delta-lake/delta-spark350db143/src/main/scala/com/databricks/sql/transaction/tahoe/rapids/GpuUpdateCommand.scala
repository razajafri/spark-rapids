/*
 * Copyright (c) 2024, NVIDIA CORPORATION.
 *
 * This file was derived from UpdateCommand.scala
 * in the Delta Lake project at https://github.com/delta-io/delta.
 * v3.3.0
 *
 * Copyright (2021) The Delta Lake Project Authors.
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

package com.databricks.sql.transaction.tahoe.rapids

import java.util.concurrent.TimeUnit

import com.databricks.sql.execution.metric.IncrementMetric
import com.databricks.sql.transaction.tahoe.{DeltaLog, DeltaOperations, DeltaTableUtils, NumRecordsStats, OptimisticTransaction, RowTracking}
import com.databricks.sql.transaction.tahoe.{MaterializedRowCommitVersion, MaterializedRowId}
import com.databricks.sql.transaction.tahoe.actions.{AddCDCFile, AddFile, FileAction}
import com.databricks.sql.transaction.tahoe.commands.{DeletionVectorUtils, DeltaCommand, DMLWithDeletionVectorsHelper, TouchedFileWithDV, UpdateCommand, UpdateMetric}
import com.databricks.sql.transaction.tahoe.files.{TahoeBatchFileIndex, TahoeFileIndex}
import com.databricks.sql.transaction.tahoe.sources.DeltaSQLConf
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkContext
import org.apache.spark.sql.{Column, Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression, Literal}
import org.apache.spark.sql.catalyst.expressions.Literal.TrueLiteral
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.metric.SQLMetrics.{createMetric, createTimingMetric}
import org.apache.spark.sql.functions.input_file_name
import org.apache.spark.sql.types.LongType

case class GpuUpdateCommand(
    gpuDeltaLog: GpuDeltaLog,
    tahoeFileIndex: TahoeFileIndex,
    target: LogicalPlan,
    updateExpressions: Seq[Expression],
    condition: Option[Expression],
    catalogTable: Option[CatalogTable])
    extends LeafRunnableCommand with DeltaCommand {

  override val output: Seq[Attribute] = {
    Seq(AttributeReference("num_affected_rows", LongType)())
  }

  override def innerChildren: Seq[QueryPlan[_]] = Seq(target)

  @transient private lazy val sc: SparkContext = SparkContext.getOrCreate()

  override lazy val metrics = Map[String, SQLMetric](
    "numAddedFiles" -> createMetric(sc, "number of files added."),
    "numAddedBytes" -> createMetric(sc, "number of bytes added"),
    "numRemovedFiles" -> createMetric(sc, "number of files removed."),
    "numRemovedBytes" -> createMetric(sc, "number of bytes removed"),
    "numUpdatedRows" -> createMetric(sc, "number of rows updated."),
    "numCopiedRows" -> createMetric(sc, "number of rows copied."),
    "executionTimeMs" ->
        createTimingMetric(sc, "time taken to execute the entire operation"),
    "scanTimeMs" ->
        createTimingMetric(sc, "time taken to scan the files for matches"),
    "rewriteTimeMs" ->
        createTimingMetric(sc, "time taken to rewrite the matched files"),
    "numAddedChangeFiles" -> createMetric(sc, "number of change data capture files generated"),
    "changeFileBytes" -> createMetric(sc, "total size of change data capture files generated"),
    "numTouchedRows" -> createMetric(sc, "number of rows touched (copied + updated)"),
    "numDeletionVectorsAdded" -> createMetric(sc, "number of deletion vectors added."),
    "numDeletionVectorsRemoved" -> createMetric(sc, "number of deletion vectors removed."),
    "numDeletionVectorsUpdated" -> createMetric(sc, "number of deletion vectors updated.")
  )

  final override def run(sparkSession: SparkSession): Seq[Row] = {
    recordDeltaOperation(tahoeFileIndex.deltaLog, "delta.dml.update") {
      val deltaLog = tahoeFileIndex.deltaLog
      gpuDeltaLog.withNewTransaction { txn =>
        DeltaLog.assertRemovable(txn.snapshot)
        if (hasBeenExecuted(txn, sparkSession)) {
          sendDriverMetrics(sparkSession, metrics)
          return Seq.empty
        }
        performUpdate(sparkSession, deltaLog, txn)
      }
      // Re-cache all cached plans(including this relation itself, if it's cached) that refer to
      // this data source relation.
      sparkSession.sharedState.cacheManager.recacheByPlan(sparkSession, target)
    }
    Seq(Row(metrics("numUpdatedRows").value))
  }

  private def performUpdate(
      sparkSession: SparkSession, deltaLog: DeltaLog, txn: OptimisticTransaction): Unit = {
    import com.databricks.sql.transaction.tahoe.implicits._

    var numTouchedFiles: Long = 0
    var numRewrittenFiles: Long = 0
    var numAddedBytes: Long = 0
    var numRemovedBytes: Long = 0
    var numAddedChangeFiles: Long = 0
    var changeFileBytes: Long = 0
    var scanTimeMs: Long = 0
    var rewriteTimeMs: Long = 0
    var numDeletionVectorsAdded: Long = 0
    var numDeletionVectorsRemoved: Long = 0
    var numDeletionVectorsUpdated: Long = 0

    val startTime = System.nanoTime()
    val numFilesTotal = txn.snapshot.numOfFiles

    val updateCondition = condition.getOrElse(Literal.TrueLiteral)
    val (metadataPredicates, dataPredicates) =
      DeltaTableUtils.splitMetadataAndDataPredicates(
        updateCondition, txn.metadata.partitionColumns, sparkSession)

    // Should we write the DVs to represent updated rows?
    val shouldWriteDeletionVectors = shouldWritePersistentDeletionVectors(sparkSession, txn)
    val candidateFiles = txn.filterFiles(metadataPredicates ++ dataPredicates,
      keepNumRecords = true /* DB always writes numRecords*/)
    val nameToAddFile = generateCandidateFileMap(deltaLog.dataPath, candidateFiles)

    scanTimeMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime)

    val filesToRewrite: Seq[TouchedFileWithDV] = if (candidateFiles.isEmpty) {
      // Case 1: Do nothing if no row qualifies the partition predicates
      // that are part of Update condition
      Nil
    } else if (dataPredicates.isEmpty) {
      // Case 2: Update all the rows from the files that are in the specified partitions
      // when the data filter is empty
      candidateFiles
        .map(f => TouchedFileWithDV(f.path, f, newDeletionVector = null, deletedRows = 0L))
    } else {
      // Case 3: Find all the affected files using the user-specified condition
      val fileIndex = new TahoeBatchFileIndex(
        sparkSession, "update", candidateFiles, deltaLog, tahoeFileIndex.path, txn.snapshot)

      val touchedFilesWithDV = if (shouldWriteDeletionVectors) {
        // Case 3.1: Find all the affected files via DV path
        val targetDf = DMLWithDeletionVectorsHelper.createTargetDfForScanningForMatches(
          sparkSession,
          target,
          fileIndex)

        // Does the target table already has DVs enabled? If so, we need to read the table
        // with deletion vectors.
        val mustReadDeletionVectors = DeletionVectorUtils.deletionVectorsReadable(txn.snapshot)

        DMLWithDeletionVectorsHelper.findTouchedFiles(
          sparkSession,
          txn,
          mustReadDeletionVectors,
          deltaLog,
          targetDf,
          fileIndex,
          updateCondition,
          opName = "UPDATE")
      } else {
        // Case 3.2: Find all the affected files using the non-DV path
        // Keep everything from the resolved target except a new TahoeFileIndex
        // that only involves the affected files instead of all files.
        val newTarget = DeltaTableUtils.replaceFileIndex(target, fileIndex)
        val data = Dataset.ofRows(sparkSession, newTarget)
        val incrUpdatedCountExpr = IncrementMetric(TrueLiteral, metrics("numUpdatedRows"))
        val pathsToRewrite =
          withStatusCode("DELTA", UpdateCommand.FINDING_TOUCHED_FILES_MSG) {
            data.filter(new Column(updateCondition))
              .select(input_file_name())
              .filter(new Column(incrUpdatedCountExpr))
              .distinct()
              .as[String]
              .collect()
          }

        // Wrap AddFile into TouchedFileWithDV that has empty DV.
        pathsToRewrite
          .map(getTouchedFile(deltaLog.dataPath, _, nameToAddFile))
          .map(f => TouchedFileWithDV(f.path, f, newDeletionVector = null, deletedRows = 0L))
          .toSeq
      }
      // Refresh scan time for Case 3, since we performed scan here.
      scanTimeMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime)
      touchedFilesWithDV
    }

    val totalActions = {
      // When DV is on, we first mask removed rows with DVs and generate (remove, add) pairs.
      val actionsForExistingFiles = if (shouldWriteDeletionVectors) {
        // When there's no data predicate, all matched files are removed.
        if (dataPredicates.isEmpty) {
          val operationTimestamp = System.currentTimeMillis()
          filesToRewrite.map(_.fileLogEntry.removeWithTimestamp(operationTimestamp))
        } else {
          // When there is data predicate, we generate (remove, add) pairs.
          val filesToRewriteWithDV = filesToRewrite.filter(_.newDeletionVector != null)
          val (dvActions, metricMap) = DMLWithDeletionVectorsHelper.processUnmodifiedData(
            sparkSession,
            filesToRewriteWithDV,
            txn.snapshot)
          metrics("numUpdatedRows").set(metricMap("numModifiedRows"))
          numDeletionVectorsAdded = metricMap("numDeletionVectorsAdded")
          numDeletionVectorsRemoved = metricMap("numDeletionVectorsRemoved")
          numDeletionVectorsUpdated = metricMap("numDeletionVectorsUpdated")
          numTouchedFiles = metricMap("numRemovedFiles")
          dvActions
        }
      } else {
        // Without DV we'll leave the job to `rewriteFiles`.
        Nil
      }

      // When DV is on, we write out updated rows only. The return value will be only `add` actions.
      // When DV is off, we write out updated rows plus unmodified rows from the same file, then
      // return `add` and `remove` actions.
      val rewriteStartNs = System.nanoTime()
      val actionsForNewFiles =
        withStatusCode("DELTA", UpdateCommand.rewritingFilesMsg(filesToRewrite.size)) {
          if (filesToRewrite.nonEmpty) {
            rewriteFiles(
              sparkSession,
              txn,
              rootPath = tahoeFileIndex.path,
              inputLeafFiles = filesToRewrite.map(_.fileLogEntry),
              nameToAddFileMap = nameToAddFile,
              condition = updateCondition,
              generateRemoveFileActions = !shouldWriteDeletionVectors,
              copyUnmodifiedRows = !shouldWriteDeletionVectors)
          } else {
            Nil
          }
        }
      rewriteTimeMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - rewriteStartNs)

      numTouchedFiles = filesToRewrite.length
      val (addActions, removeActions) = actionsForNewFiles.partition(_.isInstanceOf[AddFile])
      numRewrittenFiles = addActions.size
      numAddedBytes = addActions.map(_.getFileSize).sum
      numRemovedBytes = removeActions.map(_.getFileSize).sum

      actionsForExistingFiles ++ actionsForNewFiles
    }

    val changeActions = totalActions.collect { case f: AddCDCFile => f }
    numAddedChangeFiles = changeActions.size
    changeFileBytes = changeActions.map(_.size).sum

    metrics("numAddedFiles").set(numRewrittenFiles)
    metrics("numAddedBytes").set(numAddedBytes)
    metrics("numAddedChangeFiles").set(numAddedChangeFiles)
    metrics("changeFileBytes").set(changeFileBytes)
    metrics("numRemovedFiles").set(numTouchedFiles)
    metrics("numRemovedBytes").set(numRemovedBytes)
    metrics("executionTimeMs").set(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime))
    metrics("scanTimeMs").set(scanTimeMs)
    metrics("rewriteTimeMs").set(rewriteTimeMs)
    // In the case where the numUpdatedRows is not captured, we can siphon out the metrics from
    // the BasicWriteStatsTracker. This is for case 2 where the update condition contains only
    // metadata predicates and so the entire partition is re-written.
    val outputRows = txn.getMetric("numOutputRows").map(_.value).getOrElse(-1L)
    if (metrics("numUpdatedRows").value == 0 && outputRows != 0 &&
      metrics("numCopiedRows").value == 0) {
      // We know that numTouchedRows = numCopiedRows + numUpdatedRows.
      // Since an entire partition was re-written, no rows were copied.
      // So numTouchedRows == numUpdateRows
      metrics("numUpdatedRows").set(metrics("numTouchedRows").value)
    } else {
      // This is for case 3 where the update condition contains both metadata and data predicates
      // so relevant files will have some rows updated and some rows copied. We don't need to
      // consider case 1 here, where no files match the update condition, as we know that
      // `totalActions` is empty.
      metrics("numCopiedRows").set(
        metrics("numTouchedRows").value - metrics("numUpdatedRows").value)
      metrics("numDeletionVectorsAdded").set(numDeletionVectorsAdded)
      metrics("numDeletionVectorsRemoved").set(numDeletionVectorsRemoved)
      metrics("numDeletionVectorsUpdated").set(numDeletionVectorsUpdated)
    }
    txn.registerSQLMetrics(sparkSession, metrics)

    val finalActions = createSetTransaction(sparkSession, deltaLog).toSeq ++ totalActions
    val numRecordsStats = NumRecordsStats.fromActions(finalActions)
    val tags: Map[String, String] = totalActions.flatMap(_.tags).toMap
    val commitVersion = txn.commitIfNeeded(
      actions = finalActions,
      op = DeltaOperations.Update(condition),
      tags = tags ++ RowTracking.addPreservedRowTrackingTagIfNotSet(txn.snapshot))
    sendDriverMetrics(sparkSession, metrics)

    recordDeltaEvent(
      deltaLog,
      "delta.dml.update.stats",
      data = UpdateMetric(
        condition = condition.map(_.sql).getOrElse("true"),
        numFilesTotal,
        numTouchedFiles,
        numRewrittenFiles,
        numAddedChangeFiles,
        changeFileBytes,
        scanTimeMs,
        rewriteTimeMs,
        // We don't support deletion vectors
        numDeletionVectorsAdded = 0,
        numDeletionVectorsRemoved = 0,
        numDeletionVectorsUpdated = 0,
        numLogicalRecordsAdded = numRecordsStats.numLogicalRecordsAdded,
        numLogicalRecordsRemoved = numRecordsStats.numLogicalRecordsRemoved)
    )
  }

  /**
   * Scan all the affected files and write out the updated files.
   *
   * When CDF is enabled, includes the generation of CDC preimage and postimage columns for
   * changed rows.
   *
   * @return a list of [[FileAction]]s, consisting of newly-written data and CDC files and old
   *         files that have been removed.
   */
  private def rewriteFiles(
      spark: SparkSession,
      txn: OptimisticTransaction,
      rootPath: Path,
      inputLeafFiles: Seq[AddFile],
      nameToAddFileMap: Map[String, AddFile],
      condition: Expression,
      generateRemoveFileActions: Boolean,
      copyUnmodifiedRows: Boolean): Seq[FileAction] = {
    // Number of total rows that we have seen, i.e. are either copying or updating (sum of both).
    // This will be used later, along with numUpdatedRows, to determine numCopiedRows.
    val incrTouchedCountExpr = IncrementMetric(TrueLiteral, metrics("numTouchedRows"))

    // Containing the map from the relative file path to AddFile
    val baseRelation = buildBaseRelation(
      spark, txn, "update", rootPath, inputLeafFiles.map(_.path), nameToAddFileMap)
    val newTarget = DeltaTableUtils.replaceFileIndex(target, baseRelation.location)
    val df = Dataset.ofRows(spark, newTarget)
    val targetDf = RowTracking.preserveRowTrackingColumns(
      Dataset.ofRows(spark, newTarget), txn.snapshot)

    val rowIdAttributeOpt = MaterializedRowId.getAttribute(txn.snapshot, df)
    val rowCommitVersionAttributeOpt =
      MaterializedRowCommitVersion.getAttribute(txn.snapshot, df)
    val finalUpdateExpressions = this.updateExpressions ++
      rowIdAttributeOpt ++
      rowCommitVersionAttributeOpt.map(_ => Literal(null, org.apache.spark.sql.types.LongType))

    val finalOutput = this.target.output ++
      rowIdAttributeOpt ++
      rowCommitVersionAttributeOpt

    val targetDfWithEvaluatedCondition = {
      val evalDf = targetDf.withColumn(UpdateCommand.CONDITION_COLUMN_NAME, new Column(condition))
      val copyAndUpdateRowsDf = if (copyUnmodifiedRows) {
        evalDf
      } else {
        evalDf.filter(new Column(UpdateCommand.CONDITION_COLUMN_NAME))
      }
      copyAndUpdateRowsDf.filter(new Column(incrTouchedCountExpr))
    }

    val updatedDataFrame = UpdateCommand.withUpdatedColumns(
      finalOutput,
      finalUpdateExpressions,
      condition,
      targetDfWithEvaluatedCondition,
      UpdateCommand.shouldOutputCdc(txn))

    val addFiles = txn.writeFiles(updatedDataFrame)

    val removeFiles = if (generateRemoveFileActions) {
      val operationTimestamp = System.currentTimeMillis()
      inputLeafFiles.map(_.removeWithTimestamp(operationTimestamp))
    } else {
      Nil
    }

    addFiles ++ removeFiles
  }

  def shouldWritePersistentDeletionVectors(
      spark: SparkSession, txn: OptimisticTransaction): Boolean = {
    spark.conf.get(DeltaSQLConf.UPDATE_USE_PERSISTENT_DELETION_VECTORS) &&
      DeletionVectorUtils.deletionVectorsWritable(txn.snapshot)
  }
}
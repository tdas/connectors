/*
 * Copyright (2020-present) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.delta.standalone.internal.scan

import java.net.URI
import java.util.{NoSuchElementException, Optional}

import scala.jdk.CollectionConverters.mapAsScalaMapConverter

import java.util

import io.delta.core.internal.{DeltaScanTaskCoreImpl, DeltaStandaloneScanTaskCoreImpl}
import io.delta.standalone.DeltaScan
import io.delta.standalone.actions.{AddFile => AddFileJ}
import io.delta.standalone.core.{DeltaScanHelper, DeltaScanTaskCore}
import io.delta.standalone.data.{CloseableIterator, RowRecord}
import io.delta.standalone.expressions.Expression
import io.delta.standalone.internal.SnapshotImpl.canonicalizePath
import io.delta.standalone.internal.actions.{AddFile, MemoryOptimizedLogReplay, RemoveFile}
import io.delta.standalone.internal.util.{ConversionUtils, FileNames}

/**
 * Scala implementation of Java interface [[DeltaScan]].
 */
class DeltaScanImpl(
    replay: MemoryOptimizedLogReplay,
    scanHelper: DeltaScanHelper
  ) extends DeltaScan {

  /**
   * Whether or not the given [[AddFile]] should be returned during iteration.
   */
  protected def accept(addFile: AddFile): Boolean = true

  /**
   * This is a utility method for internal use cases where we need the filtered files
   * as their Scala instances, instead of Java.
   */
  def getFilesScala: Array[AddFile] = {
    import io.delta.standalone.internal.util.Implicits._

    getIterScala.toArray
  }

  override def getFiles: CloseableIterator[AddFileJ] = new CloseableIterator[AddFileJ] {
    private val iter = getIterScala

    override def hasNext: Boolean = iter.hasNext

    override def next(): AddFileJ = ConversionUtils.convertAddFile(iter.next())

    override def close(): Unit = iter.close()
  }

  override def getInputPredicate: Optional[Expression] = Optional.empty()

  override def getPushedPredicate: Optional[Expression] = Optional.empty()

  override def getResidualPredicate: Optional[Expression] = Optional.empty()

  /**
   * Replay Delta transaction logs and return a [[CloseableIterator]] of all [[AddFile]]s
   * that
   * - are valid delta files (i.e. they have not been removed or returned already)
   * - pass the given [[accept]] check
   */
  private def getIterScala: CloseableIterator[AddFile] = new CloseableIterator[AddFile] {
    private val iter = replay.getReverseIterator
    private val addFiles = new scala.collection.mutable.HashSet[(URI, String)]()
    private val tombstones = new scala.collection.mutable.HashSet[(URI, String)]()
    private var nextMatching: Option[AddFile] = None

    /**
     * @return the next AddFile in the log that has not been removed or returned already, or None
     *         if no such AddFile exists.
     */
    private def findNextValid(): Option[AddFile] = {
      while (iter.hasNext) {
        val (action, isCheckpoint) = iter.next()

        action match {
          case add: AddFile =>
            val canonicalizeAdd = add.copy(
              dataChange = false,
              path = canonicalizePath(add.path, replay.hadoopConf),
            )

            val alreadyDeleted = tombstones.contains(
              (canonicalizeAdd.pathAsUri,
                Option(canonicalizeAdd.deletionVector).map(_.uniqueId).orNull))
            val alreadyReturned = addFiles.contains(
              (canonicalizeAdd.pathAsUri,
                Option(canonicalizeAdd.deletionVector).map(_.uniqueId).orNull))

            if (!alreadyReturned) {
              // no AddFile will appear twice in a checkpoint so we only need non-checkpoint
              // AddFiles in the set
              if (!isCheckpoint) {
                addFiles.add((canonicalizeAdd.pathAsUri,
                  Option(canonicalizeAdd.deletionVector).map(_.uniqueId).orNull))
              }

              if (!alreadyDeleted) {
                return Some(canonicalizeAdd)
              }
            }
          // Note: `RemoveFile` in a checkpoint is useless since when we generate a checkpoint, an
          // AddFile file must be removed if there is a `RemoveFile`
          case remove: RemoveFile if !isCheckpoint =>
            val canonicalizeRemove = remove.copy(
              dataChange = false,
              path = canonicalizePath(remove.path, replay.hadoopConf))

            tombstones.add((canonicalizeRemove.pathAsUri,
              Option(canonicalizeRemove.deletionVector).map(_.uniqueId).orNull))

          case _ => // do nothing
        }
      }

      // No next valid found
      None
    }

    /**
     * Sets the [[nextMatching]] variable to the next "valid" AddFile that also passes the given
     * [[accept]] check, or None if no such AddFile file exists.
     */
    private def setNextMatching(): Unit = {
      var nextValid = findNextValid()

      while (nextValid.isDefined) {
        if (accept(nextValid.get)) {
          nextMatching = nextValid
          return
        }

        nextValid = findNextValid()
      }

      // No next matching found
      nextMatching = None
    }

    override def hasNext: Boolean = {
      // nextMatching will be empty if
      // a) this is the first time hasNext has been called
      // b) next() was just called and successfully returned a next element, setting nextMatching to
      //    None
      // c) we've run out of actions to iterate over. in this case, setNextMatching() and
      //    findNextValid() will both short circuit and return immediately
      if (nextMatching.isEmpty) {
        setNextMatching()
      }
      nextMatching.isDefined
    }

    override def next(): AddFile = {
      if (!hasNext()) throw new NoSuchElementException()
      val ret = nextMatching.get
      nextMatching = None
      ret
    }

    override def close(): Unit = {
      iter.close()
    }
  }

  override def getTasks(): CloseableIterator[DeltaScanTaskCore] = {
    new CloseableIterator[DeltaScanTaskCore] {
      private val iter = getIterScala

      override def hasNext: Boolean = iter.hasNext

      override def next(): DeltaScanTaskCore = {
        val addFile = iter.next()
        // println("Add file path: " + addFile.path)
        val tablePath = replay.snapshot.deltaLog.dataPath
        // println("Table path: " + tablePath)
        new DeltaStandaloneScanTaskCoreImpl(
          tablePath,
          FileNames.absolutePath(tablePath, addFile.path).toString,
          addFile.partitionValues,
          replay.snapshot.metadataScala.schema,
          addFile.deletionVector,
          scanHelper.getReadTimeZone(),
          scanHelper)
      }

      override def close(): Unit = iter.close()
    }
  }


  def getRows(): io.delta.standalone.utils.CloseableIterator[RowRecord] = {
    import io.delta.core.internal.utils.CloseableIteratorScala._
    getTasks()
      .asScalaClosable
      .flatMapAsCloseable(_.getDataAsRows.asScalaClosable)
      .flatMapAsCloseable(_.toRowIterator.asScalaClosable)
      .asJava
  }

}

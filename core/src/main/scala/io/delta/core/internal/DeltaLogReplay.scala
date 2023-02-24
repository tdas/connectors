package io.delta.core.internal

import java.net.URI

import io.delta.core.internal.utils.CloseableIteratorScala
import io.delta.core.internal.utils.CloseableIteratorScala._
import io.delta.standalone.actions._
import io.delta.standalone.core.LogReplayHelper
import io.delta.standalone.data.RowRecord

class DeltaLogReplay(
  logSegment: DeltaLogSegment,
  logHelper: LogReplayHelper) {

  lazy val (protocol, metadata) = loadTableProtocolAndMetadata()

  def loadTableProtocolAndMetadata(): (Protocol, Metadata) = {
    var protocol: Protocol = null
    var metadata: Metadata = null
    val iter = getActionIteratorInReverseVersionOrder

    try {
      // We replay logs from newest to oldest and will stop when we find the latest Protocol and
      // Metadata.
      iter.foreach { case (action, _) =>
        action match {
          case p: Protocol if null == protocol =>
            // We only need the latest protocol
            protocol = p

            if (protocol != null && metadata != null) {
              // Stop since we have found the latest Protocol and metadata.
              return (protocol, metadata)
            }
          case m: Metadata if null == metadata =>
            metadata = m

            if (protocol != null && metadata != null) {
              // Stop since we have found the latest Protocol and metadata.
              return (protocol, metadata)
            }
          case _ => // do nothing
        }
      }
    } finally {
      iter.close()
    }

    // Sanity check. Should not happen in any valid Delta logs.
    if (protocol == null) {
      throw new IllegalStateException(
        s"""
           |The protocol of your Delta table couldn't be recovered while reconstructing version:
           |${logSegment.version}. Did you manually delete files in the _delta_log directory?
       """.stripMargin)
    }
    if (metadata == null) {
      throw new IllegalStateException(
        s"""
           |The metadata of your Delta table couldn't be recovered while reconstructing version:
           |${logSegment.version}. Did you manually delete files in the _delta_log directory?
       """.stripMargin)
    }
    throw new IllegalStateException("should not happen")
  }

  def getAddFileIterator(addFileFilter: AddFile => Boolean): CloseableIteratorScala[AddFile] = {
    new CloseableIteratorScala[AddFile] {
      private val iter = getActionIteratorInReverseVersionOrder
      private val addFiles = new scala.collection.mutable.HashSet[URI]()
      private val tombstones = new scala.collection.mutable.HashSet[URI]()
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
              val canonicalizeAdd = new AddFile(
                canonicalizePath(add.getPath),
                add.getPartitionValues,
                add.getSize,
                add.getModificationTime,
                add.isDataChange,
                add.getStats,
                add.getTags)
              val canonicalizeAddPathUri = new URI(canonicalizeAdd.getPath)

              val alreadyDeleted = tombstones.contains(canonicalizeAddPathUri)
              val alreadyReturned = addFiles.contains(canonicalizeAddPathUri)

              if (!alreadyReturned) {
                // no AddFile will appear twice in a checkpoint so we only need non-checkpoint
                // AddFiles in the set
                if (!isCheckpoint) {
                  addFiles += canonicalizeAddPathUri
                }

                if (!alreadyDeleted) {
                  return Some(canonicalizeAdd)
                }
              }
            // Note: `RemoveFile` in a checkpoint is useless since when we generate a checkpoint, an
            // AddFile file must be removed if there is a `RemoveFile`
            case remove: RemoveFile if !isCheckpoint =>
              val canonicalizeRemovePathUri = new URI(canonicalizePath(remove.getPath))
              tombstones += canonicalizeRemovePathUri
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
          if (addFileFilter(nextValid.get)) {
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
        // b) next() was just called an successfully returned an element, setting nextMatching to
        //    None
        // c) we've run out of actions to iterate over. in this case, setNextMatching() and
        //    findNextValid() will both short circuit and return immediately
        if (nextMatching.isEmpty) {
          setNextMatching()
        }
        nextMatching.isDefined
      }

      override def next(): AddFile = {
        if (!hasNext) throw new NoSuchElementException()
        val ret = nextMatching.get
        nextMatching = None
        ret
      }

      override def close(): Unit = {
        iter.close()
      }
    }
  }

  private def getActionIteratorInReverseVersionOrder: CloseableIteratorScala[(Action, Boolean)] = {
    new CloseableIteratorScala[(Action, Boolean)] {
      private val reverseFilesIter = logSegment.allFilesReverseSorted.iterator
      private var actionIter: Option[CloseableIteratorScala[(Action, Boolean)]] = None

      /**
       * Requires that `reverseFilesIter.hasNext` is true
       */
      private def getNextIter: Option[CloseableIteratorScala[(Action, Boolean)]] = {
        val nextFile = reverseFilesIter.next()
        val iter = if (nextFile.endsWith(".json")) {
          logHelper.readJsonFile(nextFile, null)
            .asScalaCloseable.mapAsCloseable(x => (rowToAction(x), false))
        } else if (nextFile.endsWith(".parquet")) {
          logHelper.readParquetFile(nextFile, null, Array.empty)
            .asScalaCloseable.mapAsCloseable(x => (rowToAction(x), true))
        } else {
          throw new IllegalStateException(s"unexpected log file path: $nextFile")
        }
        Some(iter)
      }

      /**
       * If the current `actionIter` has no more elements, this function repeatedly reads the next
       * file, if it exists, and creates the next `actionIter` until we find a non-empty file.
       */
      private def ensureNextIterIsReady(): Unit = {
        // this iterator already has a next element, we can return early
        if (actionIter.exists(_.hasNext)) return

        actionIter.foreach(_.close())
        actionIter = None

        // there might be empty files. repeat until we find a non-empty file or run out of files
        while (reverseFilesIter.hasNext) {
          actionIter = getNextIter

          if (actionIter.exists(_.hasNext)) return

          // it was an empty file
          actionIter.foreach(_.close())
          actionIter = None
        }
      }

      override def hasNext: Boolean = {
        ensureNextIterIsReady()

        // from the semantics of `ensureNextIterIsReady()`, if `actionIter` is defined then it is
        // guaranteed to have a next element
        actionIter.isDefined
      }

      override def next(): (Action, Boolean) = {
        if (!hasNext) throw new NoSuchElementException

        if (actionIter.isEmpty) throw new IllegalStateException("Impossible")

        actionIter.get.next()
      }

      override def close(): Unit = {
        actionIter.foreach(_.close())
      }
    }
  }

  def canonicalizePath(path: String): String = {
    // TODO: fix this using the DeltaLogHelper
    path
  }

  def rowToAction(row: RowRecord): Action = {
    null
  }

  implicit def toURI(path: String): URI = new URI(path)

}

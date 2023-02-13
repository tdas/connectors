package io.delta.core

import java.io.{File, FileNotFoundException}

import scala.collection.JavaConverters._

import io.delta.core.utils.FileNames._
import io.delta.core.utils.Logging


class DeltaLogCoreImpl(val logPath: String, val logHelper: DeltaLogHelper)
  extends DeltaLogCore with Logging {

  val dataPath = new File(logPath).getParent()

  def getLatestSnapshot(): DeltaSnapshotCore = {
    val segment = getLogSegmentForVersion(None, None)
    val startingFrom = segment.checkpointVersion
      .map(v => s" starting from checkpoint version $v.").getOrElse(".")
    logInfo(s"Loading version ${segment.version}$startingFrom")
    new DeltaSnapshotCoreImpl(this, segment)
  }

  // ========== internal methods ==========

  /**
   * Get a list of files that can be used to compute a Snapshot at version `versionToLoad`, If
   * `versionToLoad` is not provided, will generate the list of files that are needed to load the
   * latest version of the Delta table. This method also performs checks to ensure that the delta
   * files are contiguous.
   *
   * @param startCheckpoint A potential start version to perform the listing of the DeltaLog,
   *                        typically that of a known checkpoint. If this version's not provided,
   *                        we will start listing from version 0.
   * @param versionToLoad A specific version to load. Typically used with time travel and the
   *                      Delta streaming source. If not provided, we will try to load the latest
   *                      version of the table.
   * @return Some LogSegment to build a Snapshot if files do exist after the given
   *         startCheckpoint. None, if there are no new files after `startCheckpoint`.
   */
  protected def getLogSegmentForVersion(
    startCheckpoint: Option[Long],
    versionToLoad: Option[Long] = None): DeltaLogSegment = {

    // List from the starting checkpoint. If a checkpoint doesn't exist, this will still return
    // deltaVersion=0.
    val newFiles = logHelper.listLogFiles(logPath).asScala
      // Pick up all checkpoint and delta files
      .filter { file => isCheckpointFile(file) || isDeltaFile(file) }
      // filter out files that aren't atomically visible. Checkpoint files of 0 size are invalid
      // TODO: Fix this.
      // .filterNot { file => isCheckpointFile(file) && file.getLen == 0 }
      // take files until the version we want to load
      .takeWhile(f => versionToLoad.forall(v => getFileVersion(f) <= v))
      .toArray

    if (newFiles.isEmpty && startCheckpoint.isEmpty) {
      throw new FileNotFoundException(s"No file found in the directory: $logPath.")
    } else if (newFiles.isEmpty) {
      // The directory may be deleted and recreated and we may have stale state in our DeltaLog
      // singleton, so try listing from the first version
      return getLogSegmentForVersion(None, versionToLoad)
    }
    val (checkpoints, deltas) = newFiles.partition(f => isCheckpointFile(f))

    // Find the latest checkpoint in the listing that is not older than the versionToLoad
    val lastCheckpoint = versionToLoad.map(CheckpointInstance(_, None))
      .getOrElse(CheckpointInstance.MaxValue)
    val checkpointFiles = checkpoints.map(f => CheckpointInstance(f))
    val newCheckpoint = getLatestCompleteCheckpointFromList(checkpointFiles, lastCheckpoint)
    if (newCheckpoint.isDefined) {
      // If there is a new checkpoint, start new lineage there.
      val newCheckpointVersion = newCheckpoint.get.version
      val newCheckpointPaths = newCheckpoint.get.getCorrespondingFiles(logPath).toSet

      val deltasAfterCheckpoint = deltas.filter { file =>
        deltaVersion(file) > newCheckpointVersion
      }
      val deltaVersions = deltasAfterCheckpoint.map(f => deltaVersion(f))

      // We may just be getting a checkpoint file after the filtering
      if (deltaVersions.nonEmpty) {
        verifyDeltaVersions(deltaVersions)
        require(deltaVersions.head == newCheckpointVersion + 1, "Did not get the first delta " +
          s"file version: ${newCheckpointVersion + 1} to compute Snapshot")
        versionToLoad.foreach { version =>
          require(deltaVersions.last == version,
            s"Did not get the last delta file version: $version to compute Snapshot")
        }
      }
      val newVersion = deltaVersions.lastOption.getOrElse(newCheckpoint.get.version)
      val newCheckpointFiles = checkpoints.filter(f => newCheckpointPaths.contains(f))
      assert(newCheckpointFiles.length == newCheckpointPaths.size,
        "Failed in getting the file information for:\n" +
          newCheckpointPaths.mkString(" -", "\n -", "") + "\n" +
          "among\n" + checkpoints.mkString(" -", "\n -", ""))

      // In the case where `deltasAfterCheckpoint` is empty, `deltas` should still not be empty,
      // they may just be before the checkpoint version unless we have a bug in log cleanup
      // val lastCommitTimestamp = deltas.last.getModificationTime

      DeltaLogSegment(
        logPath,
        newVersion,
        deltasAfterCheckpoint,
        newCheckpointFiles,
        newCheckpoint.map(_.version))
    } else {
      // No starting checkpoint found. This means that we should definitely have version 0, or the
      // last checkpoint we thought should exist (the `_last_checkpoint` file) no longer exists
      if (startCheckpoint.isDefined) {
        throw new FileNotFoundException(
          s"Checkpoint file to load version: ${startCheckpoint.get} is missing.")
      }

      val deltaVersions = deltas.map(f => deltaVersion(f))
      verifyDeltaVersions(deltaVersions)
      if (deltaVersions.head != 0) {
        new FileNotFoundException(s"${deltaFile(logPath, 0L)}: Unable " +
          s"to reconstruct state at version ${deltaVersions.last} as the " +
          s"transaction log has been truncated due to manual deletion or the log retention policy ")
      }
      versionToLoad.foreach { version =>
        require(deltaVersions.last == version,
          s"Did not get the last delta file version: $version to compute Snapshot")
      }

      val latestCommit = deltas.last
      DeltaLogSegment(
        logPath,
        deltaVersion(latestCommit), // deltas is not empty, so can call .last
        deltas,
        Nil,
        None)
    }
  }

  private def verifyDeltaVersions(versions: Array[Long]): Unit = {
    // Turn this to a vector so that we can compare it with a range.
    val deltaVersions = versions.toVector
    if (deltaVersions.nonEmpty && (deltaVersions.head to deltaVersions.last) != deltaVersions) {
      throw new IllegalStateException(s"Versions ($deltaVersions) are not contiguous.")
    }
  }

  /**
   * Given a list of checkpoint files, pick the latest complete checkpoint instance which is not
   * later than `notLaterThan`.
   */
  private def getLatestCompleteCheckpointFromList(
      instances: Array[CheckpointInstance],
      notLaterThan: CheckpointInstance): Option[CheckpointInstance] = {
    val complete = instances.filter(_.isNotLaterThan(notLaterThan)).groupBy(identity).filter {
      case (CheckpointInstance(_, None), inst) => inst.length == 1
      case (CheckpointInstance(_, Some(parts)), inst) => inst.length == parts
    }
    complete.keys.toArray.sorted.lastOption
  }


  case class CheckpointInstance(
    version: Long,
    numParts: Option[Int]) extends Ordered[CheckpointInstance] {

    /**
     * Due to lexicographic sorting, a version with more parts will appear after a version with
     * less parts during file listing. We use that logic here as well.
     */
    def isEarlierThan(other: CheckpointInstance): Boolean = {
      if (other == CheckpointInstance.MaxValue) return true
      version < other.version ||
        (version == other.version && numParts.forall(_ < other.numParts.getOrElse(1)))
    }

    def isNotLaterThan(other: CheckpointInstance): Boolean = {
      if (other == CheckpointInstance.MaxValue) return true
      version <= other.version
    }

    def getCorrespondingFiles(path: String): Seq[String] = {
      assert(this != CheckpointInstance.MaxValue, "Can't get files for CheckpointVersion.MaxValue.")
      numParts match {
        case None => checkpointFileSingular(path, version) :: Nil
        case Some(parts) => checkpointFileWithParts(path, version, parts)
      }
    }

    override def compare(that: CheckpointInstance): Int = {
      if (version == that.version) {
        numParts.getOrElse(1) - that.numParts.getOrElse(1)
      } else {
        // we need to guard against overflow. We just can't return (this - that).toInt
        if (version - that.version < 0) -1 else 1
      }
    }
  }

  object CheckpointInstance {
    def apply(path: String): CheckpointInstance = {
      CheckpointInstance(checkpointVersion(path), numCheckpointParts(path))
    }
    /*
    def apply(metadata: CheckpointMetaData): CheckpointInstance = {
      CheckpointInstance(metadata.version, metadata.parts)
    }
    */

    val MaxValue: CheckpointInstance = CheckpointInstance(-1, None)
  }
}


package io.delta.core

import java.io.Closeable

import io.delta.core.utils.FileNames


class DeltaSnapshotCoreImpl(
  val log: DeltaLogCoreImpl,
  val logSegment: DeltaLogSegment
) extends DeltaSnapshotCore {

  def scan(scanHelper: DeltaScanHelper): DeltaScanCore = new DeltaScanCoreImpl(this, scanHelper)

  // ========== internal methods ==========

  val replay = new DeltaLogReplay(logSegment, log.logHelper)
}



case class DeltaLogSegment(
  logPath: String,
  version: Long,
  deltas: Seq[String],
  checkpoints: Seq[String],
  checkpointVersion: Option[Long]) {

  lazy val allFilesReverseSorted: Seq[String] = {
    (deltas ++ checkpoints)
      .sortWith((x: String, y: String) => FileNames.getFileName(x) > FileNames.getFileName(y))
  }
}
// , lastCommitTimestamp: Long)


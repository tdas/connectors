package io.delta.core


import scala.collection.JavaConverters._

import io.delta.core.utils.{CloseableIterator, FileNames}

class DeltaScanCoreImpl(
    snapshot: DeltaSnapshotCoreImpl,
    scanHelper: DeltaScanHelper)
  extends DeltaScanCore {

  def getSplits(): CloseableIterator[DeltaScanSplitCore] = {
    new CloseableIterator[DeltaScanSplitCore] {
      private val iter = replay.getAddFileIterator(_ => true)

      override def hasNext: Boolean = iter.hasNext

      override def next(): DeltaScanSplitCore = {
        val addFile = iter.next()
        new DeltaScanSplitCoreImpl(
          FileNames.absolutePath(snapshot.log.dataPath, addFile.getPath),
          addFile.getPartitionValues.asScala.toMap,
          snapshot.replay.metadata.getSchema,
          scanHelper.getReadTimeZone(),
          scanHelper)
      }

      override def close(): Unit = iter.close()
    }
  }


  val replay = snapshot.replay



}





package io.delta.core.internal

import scala.collection.JavaConverters._

import io.delta.core.internal.utils.FileNames
import io.delta.standalone.core.{DeltaScanCore, DeltaScanHelper, DeltaScanSplitCore}
import io.delta.standalone.data.{RowRecord, RowBatch}
import io.delta.standalone.utils.CloseableIterator

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

  def getRows(): CloseableIterator[RowRecord] = {
    import io.delta.core.internal.utils.CloseableIteratorScala._
    getSplits()
      .asScalaClosable
      .flatMapAsCloseable(_.getDataAsRows.asScalaClosable)
      .flatMapAsCloseable(_.toRowIterator.asScalaClosable)
      .asJava
  }

  val replay = snapshot.replay
}





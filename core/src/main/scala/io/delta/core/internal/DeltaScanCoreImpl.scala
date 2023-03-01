package io.delta.core.internal

import scala.collection.JavaConverters._

import io.delta.core.internal.utils.FileNames
import io.delta.standalone.core.{DeltaScanCore, DeltaScanHelper, DeltaScanTaskCore}
import io.delta.standalone.data.RowRecord
import io.delta.standalone.utils.CloseableIterator

class DeltaScanCoreImpl(
    snapshot: DeltaSnapshotCoreImpl,
    scanHelper: DeltaScanHelper)
  extends DeltaScanCore {

  def getTasks(): CloseableIterator[DeltaScanTaskCore] = {
    new CloseableIterator[DeltaScanTaskCore] {
      private val iter = replay.getAddFileIterator(_ => true)

      override def hasNext: Boolean = iter.hasNext

      override def next(): DeltaScanTaskCore = {
        val addFile = iter.next()
        new DeltaScanTaskCoreImpl(
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
    getTasks()
      .asScalaClosable
      .flatMapAsCloseable(_.getDataAsRows.asScalaClosable)
      .flatMapAsCloseable(_.toRowIterator.asScalaClosable)
      .asJava
  }

  val replay = snapshot.replay
}





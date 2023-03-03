package io.delta.core

import scala.collection.JavaConverters._

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.arrow.memory.RootAllocator
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import io.delta.core.arrow.ArrowParquetReader
import io.delta.core.internal.utils.CloseableIteratorScala._
import io.delta.core.internal.utils.CloseableIteratorScalaImpl
import io.delta.core.json.JsonRow
import io.delta.standalone.core.LogReplayHelper
import io.delta.standalone.data.RowRecord
import io.delta.standalone.expressions.Expression
import io.delta.standalone.types.StructType
import io.delta.standalone.utils.CloseableIterator
import io.delta.storage.{LocalLogStore, LogStore}

// scalastyle:off println
class SimpleLogReplayHelper(
    val logStore: LogStore,
    val hadoopConf: Configuration) extends LogReplayHelper {
  def this(hadoopConf: Configuration) = this(new LocalLogStore(hadoopConf), hadoopConf)

  println("Scott > Created SimpleLogReplayHelper")

  val allocator = new RootAllocator

  override def listLogFiles(dir: String): CloseableIterator[String] = {
    logStore.listFrom(new Path(dir), hadoopConf)
      .asScalaClosable.mapAsCloseable(_.getPath.toString).asJava
  }

  override def readParquetFile(
      filePath: String,
      readSchema: StructType,
      pushdownFilters: Array[Expression]): CloseableIterator[RowRecord] = {
    ArrowParquetReader.readAsRows(filePath, readSchema, allocator)
  }

  override def readJsonFile(
      filePath: String,
      readSchema: StructType): CloseableIterator[RowRecord] = {
    val iter = logStore.read(new Path(filePath), hadoopConf)
    new CloseableIteratorScalaImpl[String](iter.asScala, iter.close).mapAsCloseable { jsonText =>
      SimpleLogReplayHelper.parseJson(jsonText)
    }.asJava
  }
}

object SimpleLogReplayHelper {
  val mapper = new ObjectMapper
  // mapper.setSerializationInclusion(Include.NON_ABSENT)
  // mapper.configure(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES, false)

  def parseJson(str: String): RowRecord = {
    new JsonRow(mapper.readTree(str).asInstanceOf[ObjectNode])
  }
}
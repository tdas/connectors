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
package io.delta.standalone.internal

// scalastyle:off
import io.delta.standalone.internal.actions.{CustomJsonIterator, SingleAction}
import io.delta.standalone.internal.util.JsonUtils
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.codehaus.jackson.map.ObjectMapper

import java.io.File


/**
 * This is the base implementation of the compute engine, we will use arrow-vector standard apis
 * to do the I/O operations here.
 *
 * The plan is the calling engine will override these methods with engine optimized implementations.
 */
class ComputeEngine {

  import org.apache.arrow.memory.RootAllocator
  import org.apache.arrow.vector.ipc.JsonFileReader

  /* Need an offheap memory allocator */
  val allocator = new RootAllocator(Int.MaxValue)

  def readJsonAsArrow(path: Path): VectorSchemaRoot = {
    // HACK: removing the fake: from the path.
    val strippedPath = new File(path.toString.substring(5))
    println(s"Reading file ${strippedPath} as an arrow table")

    import org.apache.arrow.vector._
    import org.apache.arrow.memory._
    import org.apache.arrow.vector.ipc._
    import scala.io.Source
    import scala.collection.JavaConverters._
    val jsonString = Source.fromFile(strippedPath).mkString
    val actions = JsonUtils.mapper.readValue[SingleAction](jsonString)

    VectorSchemaRoot
    println(s"actions ${actions}")
    null
  }

  // TODO: Support reading parquet files for data reads and checkpoint reads.
  def readParquetAsArrow(path: Path): VectorSchemaRoot = {
    throw new Exception("Reading parquet files is not yet supported.")
  }
}

private[internal] class SnapshotArrowImpl(
    override val hadoopConf: Configuration,
    override val path: Path,
    override val version: Long,
    override val logSegment: LogSegment,
    override val minFileRetentionTimestamp: Long,
    override val deltaLog: DeltaLogImpl,
    override val timestamp: Long,
    computeEngine: ComputeEngine = new ComputeEngine()) extends SnapshotImpl(
  hadoopConf, path, version, logSegment, minFileRetentionTimestamp, deltaLog, timestamp) {

    trait LogFileFormat

    case class JsonLogFile() extends LogFileFormat

    case class ParquetLogFile() extends LogFileFormat

    private def replayFile(
        path: Path,
        fmt: LogFileFormat): Unit = {
      val vsr: VectorSchemaRoot = fmt match {
        case j: JsonLogFile =>
          computeEngine.readJsonAsArrow(path)
        case p: ParquetLogFile =>
          computeEngine.readParquetAsArrow(path)
      }
      println("read vector schema root")
      println(vsr)
    }

    private def arrowLogReplay(
        logFiles: Seq[Path],
        checkpointFiles: Seq[Path],
        filter: Option[String] = None): Unit = {

      logFiles.reverse.foreach { file =>
        replayFile(file, JsonLogFile())
      }
    }

    override lazy val state: SnapshotImpl.State = {
      val files = super.files

      arrowLogReplay(files, Seq.empty, None)
      SnapshotImpl.State(
        Seq.empty,
        Seq.empty,
        Seq.empty,
        0L,
        0L,
        0L,
        0L
      )
    }

}

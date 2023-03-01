/*
 * Copyright (2021) The Delta Lake Project Authors.
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

package io.delta.standalone.internal.deletionvectors

import java.io.DataInputStream
import java.util.zip.CRC32

import org.apache.hadoop.fs.Path

import io.delta.standalone.internal.actions.DeletionVectorDescriptor
import io.delta.standalone.internal.util.JsonUtils

/**
 * Bitmap for a Deletion Vector, implemented as a thin wrapper around a Deletion Vector
 * Descriptor. The bitmap can be empty, inline or on-disk. In case of on-disk deletion
 * vectors, `tableDataPath` must be set to the data path of the Delta table, which is where
 * deletion vectors are stored.
 */
case class StoredDeletionVector(
    dvDescriptor: DeletionVectorDescriptor,
    tableDataPath: Option[Path] = None)  {
  require(tableDataPath.isDefined || !dvDescriptor.isOnDisk,
    "Table path is required for on-disk deletion vectors")

  def load(generateInputStream: Path => DataInputStream): RoaringBitmapArray = {
    if (isEmpty) {
      new RoaringBitmapArray()
    } else if (isInline) {
      RoaringBitmapArray.readFrom(dvDescriptor.inlineData)
    } else {
      assert(isOnDisk)

      var stream: DataInputStream = null
      try {
        stream = generateInputStream(onDiskPath.get)
        loadFromStream(stream, dvDescriptor.offset.getOrElse(0), dvDescriptor.sizeInBytes)
      } finally {
        stream.close()
      }
    }
  }

  def loadFromStream(stream: DataInputStream, offset: Int, size: Int): RoaringBitmapArray = {
    stream.skip(offset)
    val sizeAccordingToFile = stream.readInt()
    if (size != sizeAccordingToFile) {
      throw new IllegalStateException("DV size mismatch")
    }

    val buffer = new Array[Byte](size)
    stream.readFully(buffer)

    val expectedChecksum = stream.readInt()
    val actualChecksum = calculateChecksum(buffer)
    if (expectedChecksum != actualChecksum) {
      throw new IllegalStateException("DV checksum mismatch")
    }

    RoaringBitmapArray.readFrom(buffer)
  }

  /**
   * Calculate checksum of a serialized deletion vector. We are using CRC32 which has 4bytes size,
   * but CRC32 implementation conforms to Java Checksum interface which requires a long. However,
   * the high-order bytes are zero, so here is safe to cast to Int. This will result in negative
   * checksums, but this is not a problem because we only care about equality.
   */
  def calculateChecksum(data: Array[Byte]): Int = {
    val crc = new CRC32()
    crc.update(data)
    crc.getValue.toInt
  }

  def size: Int = dvDescriptor.sizeInBytes

  def cardinality: Long = dvDescriptor.cardinality

  lazy val getUniqueId: String = JsonUtils.toJson(dvDescriptor)

  def isEmpty: Boolean = dvDescriptor.isEmpty

  def isInline: Boolean = dvDescriptor.isInline

  private def isOnDisk: Boolean = dvDescriptor.isOnDisk

  /** The absolute path for on-disk deletion vectors. */
  private lazy val onDiskPath: Option[Path] = tableDataPath.map(dvDescriptor.absolutePath)
}

object StoredDeletionVector {
  /** The stored bitmap of an empty deletion vector. */
  final val EMPTY = StoredDeletionVector(DeletionVectorDescriptor.EMPTY, None)


  /** Factory for inline deletion vectors. */
  def inline(dvDescriptor: DeletionVectorDescriptor): StoredDeletionVector = {
    require(dvDescriptor.isInline)
    StoredDeletionVector(dvDescriptor, None)
  }

  /** Factory for deletion vectors. */
  def create(dvDescriptor: DeletionVectorDescriptor, tablePath: Path): StoredDeletionVector = {
    if (dvDescriptor.isOnDisk) {
      StoredDeletionVector(dvDescriptor, Some(tablePath))
    } else {
      StoredDeletionVector(dvDescriptor, None)
    }
  }
}

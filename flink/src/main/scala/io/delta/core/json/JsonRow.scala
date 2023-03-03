package io.delta.core.json

import java.sql.{Date, Timestamp}
import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode}

import io.delta.standalone.data.RowRecord
import io.delta.standalone.types.StructType

class JsonRow(rootNode: ObjectNode) extends RowRecord {
  override def getSchema: StructType = {
    throw new UnsupportedOperationException("not supported")
  }

  override def getLength: Int = {
    throw new UnsupportedOperationException("not supported")
  }

  override def isNullAt(fieldName: String): Boolean = !rootNode.hasNonNull(fieldName)

  override def getInt(fieldName: String): Int = rootNode.get(fieldName).intValue()

  override def getLong(fieldName: String): Long = rootNode.get(fieldName).longValue()

  override def getBoolean(fieldName: String): Boolean = rootNode.get(fieldName).booleanValue()

  override def getDouble(fieldName: String): Double = rootNode.get(fieldName).doubleValue()

  override def getString(fieldName: String): String = rootNode.get(fieldName).textValue()

  override def getRecord(fieldName: String): RowRecord = {
    if (!rootNode.get(fieldName).isObject) throw new ClassCastException()
    new JsonRow(rootNode.get(fieldName).asInstanceOf[ObjectNode])
  }

  override def getList[T](fieldName: String): util.List[T] = {
    if (!rootNode.get(fieldName).isArray) throw new ClassCastException()
    val list = new ArrayBuffer[Any]()
    rootNode.get(fieldName).asInstanceOf[ArrayNode].elements().asScala.foreach { node =>
      list += getValue(node)
    }
    list.asJava.asInstanceOf[util.List[T]]
  }

  override def getMap[K, V](fieldName: String): util.Map[K, V] = {
    val node = rootNode.get(fieldName)
    if (!node.isObject) throw new ClassCastException()

    val objectNode = node.asInstanceOf[ObjectNode]
    val keyValuePairs = objectNode.fields().asScala.map { entry =>
      entry.getKey -> getValue(entry.getValue)
    }
    if (keyValuePairs.isEmpty) return Map.empty[K, V].asJava // Map.of didn't compile :shrug:
    keyValuePairs.toMap.asJava.asInstanceOf[util.Map[K, V]]
  }

  override def getBigDecimal(fieldName: String): java.math.BigDecimal = {
    throw new UnsupportedOperationException("not supported")
  }

  override def getDate(fieldName: String): Date = {
    throw new UnsupportedOperationException("not supported")
  }

  override def getTimestamp(fieldName: String): Timestamp = {
    throw new UnsupportedOperationException("not supported")
  }

  override def getBinary(fieldName: String): Array[Byte] = {
    throw new UnsupportedOperationException("not supported")
  }

  override def getByte(fieldName: String): Byte = {
    throw new UnsupportedOperationException("not supported")
  }

  override def getShort(fieldName: String): Short = {
    rootNode.get(fieldName).shortValue()
  }

  override def getFloat(fieldName: String): Float = {
    throw new UnsupportedOperationException("not supported")
  }

  private def getValue(node: JsonNode): Any = {
    if (node.isBoolean) node.booleanValue()
    else if (node.isInt) node.intValue()
    else if (node.isDouble) node.doubleValue()
    else if (node.isTextual) node.textValue()
    else if (node.isObject) new JsonRow(node.asInstanceOf[ObjectNode])
    else throw new UnsupportedOperationException(s"not supported handling $node")
  }
}

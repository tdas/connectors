package io.delta.core.utils

import java.io.Closeable

import scala.collection.JavaConverters._

trait CloseableIteratorScala[T] extends Iterator[T] with Closeable {}

class CloseableIteratorScalaImpl[T](inner: Iterator[T], onClose: () => Unit)
  extends CloseableIteratorScala[T] {

  def this(jIterator: CloseableIterator[T]) = this(jIterator.asScala, jIterator.close)
  var innerClosed = false
  var innerHasNext = false

  override def hasNext: Boolean = {
    if (!innerClosed) {
      innerHasNext = inner.hasNext
      if (!innerHasNext) close()

      innerHasNext
    } else false
  }

  override def next(): T = {
    inner.next()
  }

  def close(): Unit = {
    if (!innerClosed) {
      onClose()
      innerClosed = true
    }
  }


}

object CloseableIteratorScala {

  implicit class RichCloseableIteratorScala[T](inner: CloseableIteratorScala[T]) {
    def mapAsCloseable[B](f: T => B): CloseableIteratorScala[B] = {
      new CloseableIteratorScalaImpl[B](inner.map(f), inner.close)
    }

    def asJava: CloseableIterator[T] = {
      new CloseableIterator[T] {
        override def close(): Unit = inner.close()
        override def hasNext: Boolean = inner.hasNext
        override def next(): T = inner.next()
      }
    }
  }

  implicit class RichCloseableIteratorJava[T](inner: CloseableIterator[T]) {
    def asScala: CloseableIteratorScala[T] = {
      new CloseableIteratorScalaImpl[T](inner.asScala, inner.close)
    }
  }
}

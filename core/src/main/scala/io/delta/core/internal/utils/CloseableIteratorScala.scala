package io.delta.core.internal.utils

import java.io.Closeable

import scala.collection.JavaConverters._

import io.delta.standalone.utils.CloseableIterator

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

    def flatMapAsCloseable[B](f: T => CloseableIteratorScala[B]): CloseableIteratorScala[B] = {
      new CloseableIteratorScala[B] {
        var currentIterator: CloseableIteratorScala[B] = null
        override def close(): Unit = {
          if (currentIterator != null) currentIterator.close()
        }

        override def hasNext: Boolean = {
          while ((currentIterator == null || !currentIterator.hasNext) && inner.hasNext) {
            currentIterator = f(inner.next())
          }
          currentIterator != null && currentIterator.hasNext
        }

        override def next(): B = currentIterator.next()
      }
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
      val scalaIter = new Iterator[T] {
        override def hasNext: Boolean = inner.hasNext
        override def next(): T = inner.next()
      }
      new CloseableIteratorScalaImpl[T](scalaIter, inner.close)
    }
  }

  implicit class RichIteratorJava[T](inner: java.util.Iterator[T]) {
    def asScalaClosable: CloseableIteratorScala[T] = {
      import scala.collection.JavaConverters._
      new CloseableIteratorScalaImpl[T](inner.asScala, () => {})
    }
  }

  implicit class RichIterator[T](inner: Iterator[T]) {
    def asClosable: CloseableIteratorScala[T] = {
      new CloseableIteratorScalaImpl[T](inner, () => {})
    }
  }
}

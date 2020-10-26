package parser

import scala.collection.{Iterator, mutable}
import scala.collection.Iterator._
import scala.collection.mutable.ArrayBuffer

/**
 * Iterator utility methods
 */
object IteratorUtils {

  /**
   * Iterator helper
   *
   * @param iterator source iterator
   * @tparam A type of iterator items
   */
  implicit class IteratorHelper[A](iterator: Iterator[A]) {
    def takeUntil(p: A => Boolean): Iterator[A] = new Iterator[A] {
      // scalastyle:off regex
      private var head: A = _
      private var headDefined: Boolean = false
      private var headLast: Boolean = false
      // scalastyle:on regex

      def hasNext: Boolean =
        if (headDefined) {
          true
        } else if (headLast || !iterator.hasNext) {
          false
        } else {
          head = iterator.next()
          headDefined = true
          if (p(head)) {
            headLast = true
          }

          true
        }

      def next(): A =
        if (hasNext) {
          headDefined = false
          head
        } else {
          empty.next()
        }
    }

    def sectionBy[K](f: A => K, limit: Int): Iterator[(K, Seq[A])] = {
      val iter = iterator.buffered

      new Iterator[(K, Seq[A])] {
        def hasNext: Boolean = iter.hasNext

        def next: (K, Seq[A]) = {
          val key = f(iter.head)
          val section = mutable.ListBuffer[A]()
          while (iter.hasNext && f(iter.head) == key && section.size < limit) {
            section += iter.next()
          }

          (key, section.toList)
        }
      }
    }

    def splitBy(splitWhen: A => Boolean): Iterator[Seq[A]] = {
      val iter = iterator.buffered

      new Iterator[Seq[A]] {
        override def hasNext: Boolean = iter.hasNext

        override def next(): Seq[A] = {
          val res = ArrayBuffer[A]()

          while (iter.hasNext && !splitWhen(iter.head)) {
            res.append(iter.next())
          }

          if (res.isEmpty && iter.hasNext && splitWhen(iter.head)) {
            res.append(iter.next())
          }

          res
        }
      }
    }

    def splitByIter(splitWhen: A => Boolean): Iterator[Iterator[A]] = {
        new Iterator[Iterator[A]] {
          def hasNext = iterator.hasNext

          def next: Iterator[A] = {
            val cur = iterator.takeWhile(!splitWhen(_))
            cur
          }
        }.withFilter(l => l.nonEmpty)
    }

  }

}

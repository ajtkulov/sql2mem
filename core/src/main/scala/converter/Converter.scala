package converter

import org.slf4j.LoggerFactory
import shapeless._
import shapeless.ops.traversable._
import shapeless.syntax.std.traversable._

import scala.util.Try

/**
 * http://stackoverflow.com/questions/34982555/convert-scala-liststring-listobject-into-model-hlist-tuple
 *
 * Parser trait
 *
 * @tparam Out type
 */
trait BqParse[Out] extends Parse[String, Out] {
  def apply(value: String): Option[Out]
}

/**
 * Parser companion object
 */
object BqParse {
  lazy val logger = LoggerFactory.getLogger(getClass)

  /**
   * Int parser
   */
  implicit object convertToInt extends BqParse[Int] {
    def apply(value: String): Option[Int] = Try(value.toInt).toOption
  }

  //  /**
  //   * Instant parser
  //   */
  //  implicit object convertToInstant extends BqParse[Instant] {
  //    def apply(value: String): Option[Instant] = Try(new Instant((value.toDouble * 1000).toLong)).toOption
  //  }

  /**
   * Long parser
   */
  implicit object convertToLong extends BqParse[Long] {
    def apply(value: String): Option[Long] = Try(value.toDouble.toLong).toOption
  }

  /**
   * Boolean parser
   */
  implicit object convertToBoolean extends BqParse[Boolean] {
    def apply(value: String): Option[Boolean] = Try(value.toBoolean).toOption
  }

  /**
   * String parser
   */
  implicit object convertToString extends BqParse[String] {
    def apply(value: String): Option[String] = {
      if (value == "\\N") {
        Some("")
      }
      else {
        Some(value)
      }
    }
  }

  /**
   * Double parser
   */
  implicit object convertToDouble extends BqParse[Double] {
    def apply(value: String): Option[Double] = Try(value.toDouble).toOption
  }

  implicit lazy val convertToOptionOInt: BqParse[Option[Int]] = convertToOption[Int]

  implicit lazy val convertToOptionOfDouble: BqParse[Option[Double]] = convertToOption[Double]

  implicit lazy val convertToOptionOfLong: BqParse[Option[Long]] = convertToOption[Long]

  def convertToOption[T](implicit internalConverter: BqParse[T]): BqParse[Option[T]] = new BqParse[Option[T]] {
    def apply(value: String): Option[Option[T]] = Some(internalConverter(value))
  }

}

/**
 * ParserAll trait
 *
 * @tparam Out type
 */
trait BQParseAll[Out] {
  type In <: HList

  def apply(values: In): Option[Out]

  def log(values: In): Option[Out]
}

/**
 * ParserAll companion object
 */
object BQParseAll extends ParseAll {
  type Input = String
}

/**
 * Parse from Input to monadic output
 *
 * @tparam Input input type
 * @tparam Out   output type
 */
trait Parse[Input, Out] {
  def apply(value: Input): Option[Out]
}

/**
 * ParserAll
 */
trait ParseAll {
  type Aux[I, O] = BQParseAll[O] {type In = I}

  type Input

  /**
   * HNil converter
   */
  implicit object convertHNil extends BQParseAll[HNil] {
    type In = HNil

    def apply(value: HNil): Option[HNil.type] = Some(HNil)

    def log(value: HNil): Option[HNil.type] = Some(HNil)
  }

  //scalastyle:off
  implicit def convertHList[T, HO <: HList](implicit cv: Parse[Input, T],
                                            cl: BQParseAll[HO]) = new BQParseAll[T :: HO] {
    type In = Input :: cl.In

    def apply(value: In) = value match {

      case x :: xs =>

        for {
          t <- cv(x)
          h0 <- cl(xs)
        } yield t :: h0
    }

    def log(value: In) = value match {
      case x :: xs =>
        BqParse.logger.error(s"Apply: $cv($x) = ${cv(x)}")

        for {
          t <- cv(x)
          h0 <- cl.log(xs)
        } yield t :: h0
    }
  }

  //scalastyle:on
}

/**
 * Converter
 */
trait AbsConverter {
  type Output
  type InputType
  val impl: ParseAll

  def convert[S <: HList, H <: HList](values: List[InputType])(implicit gen: Generic.Aux[Output, H], // Compute HList representation `H` of Output
                                                               parse: impl.Aux[S, H], // Generate parser of Hlist of String `S` to HList `H`
                                                               ft: FromTraversable[S] // Generate converter of `List[String]` to HList of Strings `S`
  ): Output = {
    values.toHList[S].flatMap(parse.apply).map(gen.from).getOrElse(throw new IllegalArgumentException(s"Can not convert list: ${values.mkString(", ")} into model. Size = ${values.size}. Log: ${values.toHList[S].flatMap(parse.log)}"))
  }
}

/**
 * Bq Converter
 */
trait Converter extends AbsConverter {
  val impl = BQParseAll
  type InputType = String
}

/**
 * BigQuery model converter
 */
trait BigQueryEntityConverter extends Converter {
  implicit def convertToModel[S <: HList, H <: HList](tableRow: List[String])
                                                     (implicit gen: Generic.Aux[Output, H], parse: BQParseAll.Aux[S, H], ft: FromTraversable[S]): Output = {

    convert(tableRow)
  }
}

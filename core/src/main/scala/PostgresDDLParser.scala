package parser

import scala.util.parsing.combinator._
import IteratorUtils._
import converter.BigQueryEntityConverter
import converter.BqParse._

/**
 * Parser combinators for MySQL DDLs (create table and inserts only)
 */
object PostgresDDLParser extends JavaTokenParsers {
  val id = """[a-zA-Z_0-9]+""".r
  val tableName = id ~ "." ~ id ^^ {
    case schema ~ _ ~ tableN => TableName(schema, tableN)
  } | id ^^ {
    case name => TableName("", name)
  }
  val columnName = id
  val indexName = id
  val default = """[_a-zA-Z/'\(\)[0-9]]+""".r
  val keyName = id
  val engine = id
  val charset = id
  val dataType = """[a-zA-Z]+(\([0-9,]+\))?""".r
  val statementTermination = ";"
  val columnDelimiter = """,*""".r

  val quotedStr = """\'(\\.|[^\'])*+\'""".r

  def cleanString(str: String): String = str.replaceAll("`", "")

  protected override val whiteSpace =
    """(\s|#.*|(?m)/\*(\*(?!/)|[^*])*\*/;*)+""".r

  val column = columnName ~ dataType ~
    ((("""CHARACTER SET""".r) ~ default ~ (("COLLATE".r ~ default) ?)) ?) ~
    (("COLLATE".r ~ default) ?) ~
    ("""unsigned""".r ?) ~
    ("""(?i)NULL""".r ?) ~
    ("""(?i)NOT NULL""".r ?) ~
    ("""(?i)AUTO_INCREMENT""".r ?) ~
    ((("""(?i)DEFAULT""".r) ~ (quotedStr | default)) ?) ~
    ("""(?i)ON UPDATE CURRENT_TIMESTAMP""".r ?) ~
    (("COMMENT".r ~ quotedStr) ?) ~
    columnDelimiter

  val uniqueOrPk = ("""(?i)(PRIMARY|UNIQUE)""".r ?) ~ ("""(?i)KEY""".r) ~
    (keyName ?) ~ "(" ~ columnName ~ (("\\([0-9]+\\)".r) ?) ~ ((columnDelimiter ~ columnName) *) ~ ")" ~ (("USING".r ~ default) ?) ~ columnDelimiter ^^ {
    case kind ~ _ ~ name ~ "(" ~ column ~ _ ~ others ~ ")" ~ _ ~ _ =>
      kind match {
        case Some(x) if x.equalsIgnoreCase("primary") => PrimaryKey(column)
        case Some(x) if x.equalsIgnoreCase("unique") => UniqueKey(name, column)
        case None => Key(name, column)
      }
  }

  val fk =
    """(?i)CONSTRAINT""".r ~ keyName ~ """FOREIGN KEY""".r ~
      "(" ~ columnName ~ ((("," ~ columnName) *) ?) ~ ")" ~
      """(?i)REFERENCES""".r ~
      tableName ~ "(" ~ columnName ~ ((("," ~ columnName) *) ?) ~ ")" ~
      (("ON DELETE NO ACTION ON UPDATE NO ACTION".r) ?) ~
      (("ON DELETE CASCADE ON UPDATE CASCADE".r) ?) ~
      (("ON UPDATE CURRENT_TIMESTAMP".r) ?) ~
      columnDelimiter ^^ {
      case _ ~ keyName ~ _ ~ "(" ~ columnName ~ columnNames ~ ")" ~ _ ~
        tableName ~ "(" ~ extColumn ~ extColumns ~ ")" ~ _ ~ _ ~ _ ~ _ =>
        ForeignKey(keyName, columnName, tableName.fullName, extColumn)
    }

  val constraint = (uniqueOrPk | fk)

  val tableMetaInfo =
    """(?i)ENGINE=""".r ~ engine ~
      ("AUTO_INCREMENT=[0-9]+".r ?) ~
      """(?i)DEFAULT CHARSET=""".r ~ charset ~
      (("""COLLATE=""".r ~ default) ?) ~
      (("""COMMENT=""".r ~ quotedStr) ?)

  val createTable: Parser[Command] = ("""(?i)CREATE TABLE""".r) ~ ("""(?i)IF NOT EXISTS""".r ?) ~ tableName ~
    "(" ~ (column *) ~ (constraint *) ~ ")" ~ (tableMetaInfo ?) ^^ {
    case _ ~ _ ~ name ~ "(" ~ columns ~ constraints ~ ")" ~ meta => {
      val columnsData = columns map {
        case colName ~ colType ~ charSet ~ collate ~ unsigned ~ nnull ~ notNull ~ autoInc ~ isDefault ~ onUpdate ~ _ ~ _ =>
          Column(cleanString(colName), colType, notNull.isDefined,
            autoInc.isDefined, isDefault.map(_._2))
      }
      Table(cleanString(name.fullName), columnsData, constraints.toSet)
    }
  }

  val dropTable: Parser[Command] = "(?i)DROP TABLE".r ~ ("IF EXISTS".r ?) ~ tableName ^^ { case _ ~ _ ~ tableName => DropTable(tableName.fullName) }

  val lockTable: Parser[Command] = "(?i)LOCK TABLES".r ~ tableName ~ "WRITE" ^^ { case _ => LockTable() }

  val nullValue: Parser[Value] = "NULL".r ^^ (_ => NullValue())
  val decimalValue: Parser[Value] = floatingPointNumber ^^ (value => if (value.contains(".")) DoubleValue(value.toDouble) else IntValue(value.toLong))
  val stringValue: Parser[Value] = quotedStr ^^ (value => StringValue(value))

  val value: Parser[Value] = nullValue | decimalValue | stringValue ^^ identity

  val argumentValues = "(" ~ value ~ (("," ~ value) *) ~ ")" ^^ {
    case _ ~ value ~ values ~ _ => Values(List(value) ++ values.map(_._2))
  }

  val insertValues: Parser[InsertValues] = "INSERT INTO".r ~ tableName ~ "VALUES".r ~ argumentValues ~ (("," ~ argumentValues) *) ^^ {
    case _ ~ tableName ~ _ ~ arg ~ args => InsertValues(tableName.fullName, List(arg) ++ args.map(_._2))
  }

  val delimiter: Parser[Command] = "DELIMITER".r ^^ (_ => Delimiter())

  val unlock: Parser[Command] = "UNLOCK TABLES".r ^^ (_ => UnlockTable())

  val empty: Parser[Command] = "".r ^^ (_ => Empty())

  val statement: Parser[Command] = (createTable | dropTable | lockTable | insertValues | unlock | delimiter | setExpr | alterTable | empty) ~ statementTermination ^^ {
    case res ~ _ => res
  }

  val setExpr: Parser[Command] = "SET".r ~ id ~ "=".r ~ id ^^ {
    case _ => SetExpr()
  } | "SET".r ~ id ~ "=".r ~ quotedStr ^^ {
    case _ => SetExpr()
  }

  val alterTable: Parser[Command] = "ALTER TABLE" ~ tableName ~ "OWNER TO" ~ id ^^ {
    case _ => AlterTable()
  }

  val program = statement *

  def parse(sql: String): ParseResult[Set[Command]] = parseAll(program, sql.split("\n").filterNot(_.startsWith("--")).mkString("\n")) map (_.toSet)

  def parseFile(fileName: String): Iterator[(Command, String)] = scala.io.Source.fromFile(fileName).getLines().filterNot(_.startsWith("--"))
    .splitBy(line => line.isEmpty || line.startsWith("COPY ")).map(lines => {
    val str = lines.mkString("\n")
    (str, parse(str))
  }).flatMap { case (str, parsed) => if (parsed.successful) parsed.get.toList.map(p => (p, str)) else List() }

//  def parseData(fileName: String, tableName: String, converter: BigQueryEntityConverter) = {
  def parseData(fileName: String, tableName: String) = {
//    scala.io.Source.fromFile(fileName).getLines().filterNot(_.startsWith("--"))
    scala.io.Source.fromFile(fileName).getLines().filterNot(_.startsWith("--")).splitByIter(line => line.isEmpty)

//    Iterator(1,2,3).take

//      .splitByIter(line => line.isEmpty).map(iter => iter.buffered).filter(x => x.hasNext && x.head == "COPY " && x.head.contains(tableName))
//      .splitByIter(line => !line.isEmpty).map(iter => iter.buffered).filter(x => x.hasNext && x.head == "COPY ")
  }


////  def parseData(fileName: String, tableName: String, converter: BigQueryEntityConverter) = {
//  def parseData(fileName: String, tableName: String) = {
//    scala.io.Source.fromFile(fileName).getLines().filterNot(_.startsWith("--"))
////      .splitByIter(line => line.isEmpty).map(iter => iter.buffered).filter(x => x.hasNext && x.head == "COPY " && x.head.contains(tableName))
//      .splitByIter(line => line.isEmpty).map(iter => iter.buffered).filter(x => x.hasNext && x.head == "COPY ")
//  }

//  def readDataFromIter[T](iter: Iterator[String], converter: BigQueryEntityConverter)(implicit gen: shapeless.Generic.Aux[converter.Output,T]): Iterator[converter.Output] = {
//    iter.map(item => converter.convertToModel(item.split("\\.").toList))
//  }

  def parseInsert(insert: String): Option[InsertValues] = {
    val parsed: ParseResult[InsertValues] = parse(insertValues, insert)
    if (parsed.successful) {
      Some(parsed.get)
    } else {
      None
    }
  }
}

package parser

import scala.util.parsing.combinator._
import IteratorUtils._

/**
 * Parser combinators for MySQL DDLs (create table and inserts only)
 */
object MysqlDDLParser extends JavaTokenParsers {
  val tableName = """(?!(?i)KEY)(?!(?i)PRIMARY)(?!(?i)UNIQUE)(`)?[a-zA-Z_0-9]+(`)?""".r
  val columnName = tableName
  val indexName = tableName
  val default = """[_a-zA-Z/'\(\)[0-9]]+""".r
  val keyName = tableName
  val engine = tableName
  val charset = tableName
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
        ForeignKey(keyName, columnName, tableName, extColumn)
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
      Table(cleanString(name), columnsData, constraints.toSet)
    }
  }

  val dropTable: Parser[Command] = "(?i)DROP TABLE".r ~ ("IF EXISTS".r ?) ~ tableName ^^ { case _ ~ _ ~ tableName => DropTable(tableName) }

  val lockTable: Parser[Command] = "(?i)LOCK TABLES".r ~ tableName ~ "WRITE" ^^ { case _ => LockTable() }

  val nullValue: MysqlDDLParser.Parser[Value] = "NULL".r ^^ (_ => NullValue())
  val decimalValue: MysqlDDLParser.Parser[Value] = floatingPointNumber ^^ (value => if (value.contains(".")) DoubleValue(value.toDouble) else IntValue(value.toLong))
  val stringValue: MysqlDDLParser.Parser[Value] = quotedStr ^^ (value => StringValue(value))

  val value: MysqlDDLParser.Parser[Value] = nullValue | decimalValue | stringValue ^^ identity

  val argumentValues = "(" ~ value ~ (("," ~ value) *) ~ ")" ^^ {
    case _ ~ value ~ values ~ _ => Values(List(value) ++ values.map(_._2))
  }

  val insertValues: Parser[InsertValues] = "INSERT INTO".r ~ tableName ~ "VALUES".r ~ argumentValues ~ (("," ~ argumentValues) *) ^^ {
    case _ ~ tableName ~ _ ~ arg ~ args => InsertValues(tableName, List(arg) ++ args.map(_._2))
  }

  val delimiter: Parser[Command] = "DELIMITER".r ^^ (_ => Delimiter())

  val unlock: Parser[Command] = "UNLOCK TABLES".r ^^ (_ => UnlockTable())

  val empty: Parser[Command] = "".r ^^ (_ => Empty())

  val statement: Parser[Command] = (createTable | dropTable | lockTable | insertValues | unlock | delimiter | empty) ~ statementTermination ^^ {
    case res ~ _ => res
  }

  val program = statement *

  def parse(sql: String): ParseResult[Set[Command]] = parseAll(program, sql.split("\n").filterNot(_.startsWith("--")).mkString("\n")) map (_.toSet)

  def parseFile(fileName: String): Iterator[(Command, String)] = scala.io.Source.fromFile(fileName).getLines().filterNot(_.startsWith("--"))
    .splitBy(line => line.isEmpty || line.startsWith("INSERT INTO")).map(lines => {
    val str = lines.mkString("\n")
    (str, parse(str))
  }).flatMap { case (str, parsed) => if (parsed.successful) parsed.get.toList.map(p => (p, str)) else List() }

  def parseInsert(insert: String): Option[InsertValues] = {
    val parsed: MysqlDDLParser.ParseResult[InsertValues] = parse(insertValues, insert)
    if (parsed.successful) {
      Some(parsed.get)
    } else {
      None
    }
  }
}

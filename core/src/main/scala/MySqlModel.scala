package parser

/**
  * DDL comand
  */
sealed trait Command

/**
  * Create table command
  *
  * @param name        table name
  * @param columns     columns
  * @param constraints table constraints
  */
final case class Table(name: String, columns: List[Column], constraints: Set[Constraint]) extends Command

/**
  * Drop table command
  *
  * @param tableName tableName
  */
final case class DropTable(tableName: String) extends Command

/**
  * Lock table command
  */
final case class LockTable() extends Command

/**
  * Unlock table command
  */
final case class UnlockTable() extends Command

/**
  * Delimiter command (begin/end of trigger)
  */
final case class Delimiter() extends Command

/**
  * Empty command
  */
final case class Empty() extends Command

final case class SetExpr() extends Command

final case class AlterTable() extends Command

/**
  * Column definition
  *
  * @param name       name
  * @param datatype   type
  * @param notNull    not null property
  * @param autoInc    auto inc
  * @param defaultVal default value
  */
final case class Column(name: String, datatype: String, notNull: Boolean,
                        autoInc: Boolean, defaultVal: Option[String]) {
  def escapeName: String = {
    if (name == "type") {
      "`type`"
    } else if (name == "class") {
      "`class`"
    } else {
      name
    }
  }
}

/**
  * Constraint marker
  */
sealed trait Constraint

/**
  * Unique key
  *
  * @param name   name
  * @param column column
  */
final case class UniqueKey(name: Option[String], column: String) extends Constraint

/**
  * Primary key
  *
  * @param column column
  */
final case class PrimaryKey(column: String) extends Constraint

/**
  * Foreign key
  *
  * @param name          name
  * @param column        column
  * @param foreignTable  table
  * @param foreignColumn column
  */
final case class ForeignKey(name: String, column: String, foreignTable: String, foreignColumn: String) extends Constraint

/**
  * Key
  *
  * @param name   name
  * @param column column
  */
final case class Key(name: Option[String], column: String) extends Constraint

/**
  * Value marker
  */
sealed trait Value

/**
  * Int value
  *
  * @param value value
  */
final case class IntValue(value: Long) extends Value

/**
  * Double value
  *
  * @param value value
  */
final case class DoubleValue(value: Double) extends Value


/**
  * Null value
  */
final case class NullValue() extends Value

/**
  * String value
  *
  * @param value value
  */
final case class StringValue(value: String) extends Value

/**
  * Insert arguments
  *
  * @param values values
  */
case class Values(values: List[Value])

/**
  * Insert value command
  *
  * @param table  table
  * @param values values
  */
case class InsertValues(table: String, values: List[Values]) extends Command {
  val tableNameTrimmed = table.filterNot(_ == '`')
}

case class TableName(schema: String, name: String) {
  def fullName: String = s"$schema.$name"
}

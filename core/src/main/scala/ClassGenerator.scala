package parser

object ClassGenerator {
  def genType(columnType: String): String = {
    columnType match {
      case "text" => "String"
      case "integer" => "Int"
      case _ => "???"
    }
  }

  def genField(column: Column): String = {
    s"${column.name}: ${genType(column.datatype)}"
  }

  def generate(table: Table): String = {

    val className = table.name.split("\\.").last
    s"""case class $className(${table.columns.map(genField).mkString(", ")}) {}
       |
       |object ${className}Convert extends BigQueryEntityConverter {
       |  override type Output = $className
       |}
       |
       |""".stripMargin
  }
}

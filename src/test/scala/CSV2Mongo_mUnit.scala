
import scala.util.Success


class CSV2Mongo_mUnit extends munit.FunSuite {

  val pathCsv: String = "src/test/scala/fileCsvTest/csvteste.csv"
  val repeatSeparator: Option[String] = Option(",")
  val pathConvTable: Option[String] = Option("src/test/scala/fileCsvTest/Table.txt")
  val fieldArray: Option[String] = Option("Author")
  val hasHeader: Boolean = true
  val noUpDate: Boolean = false

  val dataReader = new CSV2Mongo()

  val database: String = "testUnit-mongoDb"
  val collection: String = "csvToMongo-TesteUnit"
  val host: Option[String] = Option("localhost")
  val port: Option[Int] = Option(27017)
  val user: Option[String] = None
  val password: Option[String] = None
  val total: Option[Int] = Option(0)
  val append: Boolean = false


  test("Obtained data from CSV file") {
    assert(dataReader.exportCSV2Mongo(pathCsv, database, collection, host, port, user, password, repeatSeparator, pathConvTable, fieldArray, total, append, hasHeader, noUpDate).isSuccess)
  }

  test(s"Connected to mongodb and inserted data into base: '$database' and collection: '$collection'") {
    assert(dataReader.exportCSV2Mongo(pathCsv, database, collection, host, port, user, password, repeatSeparator, pathConvTable, fieldArray, total, append, hasHeader, noUpDate) match {
      case Success(value) => println(s"Collection $collection successfully connected! - $value")
        true
      case _ => ()
      false
    })
  }
}
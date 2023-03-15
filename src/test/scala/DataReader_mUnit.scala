import csv2mongodb.DataReader
import mongodb.MongoExport
import org.json4s.DefaultFormats
import org.json4s.native.Json

import scala.util.Success

class DataReader_mUnit extends munit.FunSuite {

  val pathCsv: String = "src/test/scala/fileCsvTest/csvteste.csv"
  val repeatSeparator: String = ","
  val pathConvTable: String = "src/test/scala/fileCsvTest/Table.txt"
  val fieldArray: String = "Author"
  val hasHeader: Boolean = true
  val noUpDate: Boolean = false

  val dataReader = new DataReader(pathCsv, repeatSeparator, pathConvTable, fieldArray, hasHeader, noUpDate)

  val database: String = "testUnit-mongoDb"
  val collection: String = "csvToMongo-TesteUnit"
  val host: Option[String] = Option("172.17.1.71")
  val port: Option[Int] = Option(27017)
  val user: Option[String] = None
  val password: Option[String] = None
  val append: Boolean = false

  val mongoReader: MongoExport = new MongoExport(database, collection, host, port, user, password, append)

  test("Obtained data from CSV file") {
    assert(dataReader.reader().isSuccess)
  }

  test(s"Connected to mongodb and inserted data into base: '$database' and collection: '$collection'") {
    assert(dataReader.reader() match {
      case Success(value) =>
        val k: Array[String] = value.map(f => Json(DefaultFormats).write(f.toMap))
        k.foreach(f => mongoReader.insertDocument(f))
        true
      case _ => false
    })
  }
}
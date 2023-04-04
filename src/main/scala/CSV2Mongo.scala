import org.apache.commons.csv.{CSVFormat, CSVRecord}
import org.json4s.DefaultFormats
import org.json4s.native.Json

import java.io.FileReader
import java.util
import java.util.Date
import scala.io.{BufferedSource, Source}
import scala.util.{Failure, Success, Try}

class CSV2Mongo() {

  def exportCSV2Mongo(csv: String,
                      database: String,
                      collection: String,
                      host: Option[String],
                      port: Option[Int],
                      user: Option[String],
                      password: Option[String],
                      repeatSeparator: Option[String],
                      convTable: Option[String],
                      fieldArray: Option[String],
                      total: Option[Int],
                      append: Boolean,
                      noUpDate: Boolean,
                      hasHeader: Boolean): Try[Unit] = {
    Try {
      val mongoReader: MongoDB = new MongoDB(database, collection, host, port, user, password, total, append)

      val jsonDataList: Array[String] = readerCSV(csv, repeatSeparator.getOrElse(","), convTable.getOrElse(""), fieldArray.getOrElse(""), hasHeader, noUpDate) match {
        case Success(value) => value.map(f => Json(DefaultFormats).write(f.toMap))
        case Failure(exception) => throw new Exception(exception.getMessage)
      }
      jsonDataList.foreach(f => mongoReader.insertDocument(f))
    }
  }


  private def readerCSV(pathCsv: String, repeatSeparator: String = ",", convTable: String, fieldArray: String, hasHeader: Boolean, noUpDate: Boolean): Try[Array[Array[(String, AnyRef)]]] = {
    Try {
      val fileCsv: FileReader = new FileReader(pathCsv)
      val rowsCsv: util.List[CSVRecord] = CSVFormat.DEFAULT.parse(fileCsv).getRecords

      val convertedData = if (hasHeader) {
        rowsCsv.get(0)
        if (convTable.nonEmpty) {
          val convTableHeaders: Map[String, String] = getConversionTable(Option(convTable)).get
          searchData(rowsCsv, convTableHeaders, convTable.nonEmpty, hasHeader)
        } else {
          val convListToMap: Map[String, String] = rowsCsv.get(0).values().toList.zipWithIndex.map {
            case (v, i) => (v, i.toString)
          }.toMap
          searchData(rowsCsv, convListToMap, convTable.nonEmpty, hasHeader)
        }
      } else {
        if (convTable.nonEmpty) {
          val convTableHeaders: Map[String, String] = getConversionTable(Option(convTable)).get
          searchData(rowsCsv, convTableHeaders, convTable.nonEmpty, hasHeader)
        } else {
          val convTableHeaders: Map[String, String] = Map[String, String]()
          searchData(rowsCsv, convTableHeaders, convTable.nonEmpty, hasHeader)
        }
      }
      checkFieldNoUpdDate(convertedData, repeatSeparator, fieldArray, noUpDate)
    }
  }

  private def searchData(rowsAll: util.List[CSVRecord], reader: Map[String, String], hasConvTable: Boolean, hasHeader: Boolean): Array[Array[(String, String)]] = {

    val rowsCsv = if (hasHeader) {
      rowsAll.subList(1, rowsAll.size())
    } else {
      rowsAll.subList(0, rowsAll.size())
    }
    val readerCsv = rowsAll.get(0)
    val valuesRowsCsv: Array[Array[String]] = rowsCsv.toArray.zipWithIndex.map(f => rowsCsv.get(f._2).values())
    val dataKeysAndValuesCsv: Array[Array[(String, String)]] = valuesRowsCsv.map(f => readerCsv.values().zip(f))
    val dataConvKeysAndValues: Array[Array[(String, String)]] = dataKeysAndValuesCsv.map(y => y.filter(f => reader.contains(f._1)))

    if (hasHeader && !hasConvTable) return dataKeysAndValuesCsv
    val data: Array[Array[(String, String)]] = dataKeysAndValuesCsv.map(h => h.zipWithIndex.map {
      case (q, b) =>
        (q._1.replace(q._1, b.toString), q._2)
    })

    if (hasConvTable) {
      if (hasHeader && hasConvTable) {
        dataConvKeysAndValues.map(f => f.map(h => (h._1.replace(h._1, reader(h._1)), h._2)))
      } else {
        data.map(f => f.map(h => (h._1.replace(h._1, reader(h._1)), h._2)))
      }
    } else {
      data
    }
  }

  private def getConversionTable(convTable: Option[String]): Option[Map[String, String]] = {
    convTable.map {
      cTable =>
        val src: BufferedSource = Source.fromFile(cTable, "utf-8")
        val ret: Map[String, String] = src.getLines().foldLeft(Map[String, String]()) {
          case (map, line) =>
            line.trim match {
              case "" => map
              case l =>
                val split = l.split(" *=> *", 2)
                if (split.length != 2) throw new IllegalArgumentException(s"Conversion table error: $l")
                map + (split(0) -> split(1))
            }
        }
        src.close()
        ret
    }
  }

  private def checkFieldNoUpdDate(datalListFinal: Array[Array[(String, String)]], repeatSeparator: String, fieldArray: String, noUpDate: Boolean): Array[Array[(String, AnyRef)]] = {

    val dataListFinalRefactored = if (noUpDate) {
      datalListFinal
    } else {
      val dataWithNewUpdd = datalListFinal.map(f => f.dropWhile(h => h._1 == "_updd"))
      dataWithNewUpdd.map(f => f.appended("_updd", new Date().toString))
    }
    checkArrayFields(dataListFinalRefactored, repeatSeparator, fieldArray)
  }

  private def checkArrayFields(data: Array[Array[(String, String)]], repeatSeparator: String, fieldArray: String): Array[Array[(String, AnyRef)]] = {

    val fieldArrayList: Array[String] = if (fieldArray.nonEmpty) fieldArray.split(",") else Array()
    data.map(f => f.map(h => if (fieldArrayList.contains(h._1)) (h._1, h._2.split(repeatSeparator)) else h))
  }
}

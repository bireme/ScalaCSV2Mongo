import mongodb.MongoExport
import org.json4s.DefaultFormats
import org.json4s.native.Json
import org.mongodb.scala.bson.BsonValue

import java.util.Date
import scala.util.{Failure, Success, Try}

case class InputParameters_CompMongoCol(database_from1: String,
                                        collection_from1: String,
                                        collection_from2: String,
                                        collection_out: String,
                                        database_from2: Option[String],
                                        database_out: Option[String],
                                        host_from1: Option[String],
                                        port_from1: Option[Int],
                                        host_from2: Option[String],
                                        port_from2: Option[Int],
                                        host_out: Option[String],
                                        port_out: Option[Int],
                                        user_from1: Option[String],
                                        password_from1: Option[String],
                                        user_from2: Option[String],
                                        password_from2: Option[String],
                                        user_out: Option[String],
                                        password_out: Option[String],
                                        total: Option[Int],
                                        fields: Option[String],
                                        noUpDate: Boolean,
                                        append: Boolean)


class CompareDocsColls {

  private def compareDocsColls(parameters: InputParameters_CompMongoCol): Try[Unit] = {
    Try {
      val mongo_from1: MongoExport = new MongoExport(parameters.database_from1, parameters.collection_from1, parameters.host_from1, parameters.port_from1, parameters.user_from1, parameters.password_from1, parameters.append)
      val mongo_from2: MongoExport = new MongoExport(parameters.database_from2.getOrElse(""), parameters.collection_from2, parameters.host_from2, parameters.port_from2, parameters.user_from2, parameters.password_from2, parameters.append)
      val mongo_out: MongoExport = new MongoExport(parameters.database_out.getOrElse(""), parameters.collection_out, parameters.host_out, parameters.port_out, parameters.user_out, parameters.password_out, parameters.append)

      val listDoc1 = mongo_from1.findAll
      val listDoc2 = mongo_from2.findAll

      //val docsDiff2 = listDoc2.map(f => f.filterNot(h => if (h._1 != "Refid") listDoc1.exists(doc1 => doc1.exists(field1 => h.equals(field1))) else false))
      val docsDiff1 = listDoc1.map(f => f.filter(h => if (h._1 != "Refid") !listDoc2.exists(doc2 => doc2.exists(field2 => h.equals(field2))) else true))
      val k = docsDiff1.filterNot(f => f.size == 1 && f.contains("Refid"))

      k.foreach(f => mongo_out.insertDocument(Json(DefaultFormats).write(f.toMap.map(f => (f._1, getValue(f._2))))))
    }
  }

  def getValue(f: BsonValue): String = {

    f match {
      case f if f.isObjectId => f.asObjectId().getValue.toString
      case _ => f.asString().getValue
    }
  }
}

object CompareDocsColls {

  private def usage(): Unit = {
    System.err.println("\nEnter the input parameters:")
    System.err.println("-database_from1=<name>                ~ required     - MongoDB database one name")
    System.err.println("-collection_from1=<name>              ~ required     - MongoDB database collection one name")
    System.err.println("-collection_from2=<name>              ~ required     - MongoDB database collection two name")
    System.err.println("-collection_out=<name>                ~ required     - MongoDB database collection out name")
    System.err.println("[-database_from2=<number>]            - MongoDB database two name")
    System.err.println("[-database_out=<name>])               - MongoDB database out name")
    System.err.println("[-host_from1=<pwd>]                   - MongoDB server name. Default value is 'localhost'")
    System.err.println("[-port_from1=<str>]                   - MongoDB server port number. Default value is 27017")
    System.err.println("[-host_from2=<path>]                  - MongoDB server name. Default value is 'localhost'")
    System.err.println("[-port_from2=<str>]                   - MongoDB server port number. Default value is 27017")
    System.err.println("[-host_out]                           - MongoDB server name. Default value is 'localhost'")
    System.err.println("[-port_out]                           - MongoDB server port number. Default value is 27017")
    System.err.println("[-user_from1]                         - MongoDB user name")
    System.err.println("[-password_from1]                     - MongoDB user password")
    System.err.println("[-user_from2]                         - MongoDB user name")
    System.err.println("[-password_from2]                     - MongoDB user password")
    System.err.println("[-user_out]                           - MongoDB user name")
    System.err.println("[-password_out]                       - MongoDB user password")
    System.err.println("[-total]                              - If present, take total documents")
    System.err.println("[-fields]                             - Conversion field string to array")
    System.err.println("[-noUpDate]                           - If present, it will not create de update date field (_updd), otherwise it will be created")
    System.err.println("[-append]                             - If absent, it will append documents into the MongoDB database, otherwise the database will be previously erased")
    System.exit(1)
  }

  def main(args: Array[String]): Unit = {

    if (args.length < 4) usage()

    val parameters: Map[String, String] = args.foldLeft[Map[String, String]](Map()) {
      case (map, par) =>
        val split = par.split(" *= *", 2)
        if (split.size == 1) map + ((split(0).substring(2), ""))
        else map + (split(0).substring(1) -> split(1))
    }

    if (!Set("database_from1", "collection_from1", "collection_from2", "collection_out").forall(parameters.contains)) {
      System.err.println("\n~ Required parameter")
      usage()
    }

    val listInputParam: List[String] = List("database_from1", "collection_from1", "collection_from2", "collection_out", "database_from2", "database_out", "host_from1",
      "port_from1", "host_from2", "port_from2", "host_out", "port_out", "user_from1", "password_from1", "user_from2", "password_from2", "user_out", "password_out",
      "total", "fields", "noUpdDate", "append")

    val paramsInvalid: List[String] = parameters.keys.dropWhile(listInputParam.contains).toList

    if (paramsInvalid.nonEmpty) {
      invalidParamWarning(paramsInvalid.head)
    }

    val database_from1: String = parameters("database_from1")
    val collection_from1: String = parameters("collection_from1")
    val collection_from2: String = parameters("collection_from2")
    val collection_out: String = parameters("collection_out")
    val database_from2: Option[String] = parameters.get("database_from2")
    val database_out: Option[String] = parameters.get("database_out")
    val host_from1: Option[String] = parameters.get("host_from1")
    val port_from1: Option[Int] = parameters.get("port_from1").flatMap(_.toIntOption)
    val host_from2: Option[String] = parameters.get("host_from2")
    val port_from2: Option[Int] = parameters.get("port_from2").flatMap(_.toIntOption)
    val host_out: Option[String] = parameters.get("host_out")
    val port_out: Option[Int] = parameters.get("port_out").flatMap(_.toIntOption)
    val user_from1: Option[String] = parameters.get("user_from1")
    val password_from1: Option[String] = parameters.get("password_from1")
    val user_from2: Option[String] = parameters.get("user_from2")
    val password_from2: Option[String] = parameters.get("password_from2")
    val user_out: Option[String] = parameters.get("user_out")
    val password_out: Option[String] = parameters.get("password_out")
    val total: Option[Int] = parameters.get("total").flatMap(_.toIntOption)
    val fields: Option[String] = parameters.get("fields")
    val noUpDate: Boolean = parameters.contains("noUpDate")
    val append: Boolean = parameters.contains("append")

    val params: InputParameters_CompMongoCol = InputParameters_CompMongoCol(database_from1, collection_from1, collection_from2,
      collection_out, database_from2, database_out, host_from1, port_from1, host_from2, port_from2, host_out, port_out,
      user_from1, password_from1, user_from2, password_from2, user_out, password_out, total, fields, noUpDate, append)
    val startDate: Date = new Date()
    (new CompareDocsColls).compareDocsColls(params) match {
      case Success(_) =>
        println(timeAtProcessing(startDate))
        System.exit(0)
      case Failure(exception) =>
        println(exception.getMessage)
        System.exit(1)
    }
  }

  private def invalidParamWarning(param: String): Unit = {
    System.err.println(s"\n~ Parameter '$param' does not exist!")
    usage()
  }

  private def timeAtProcessing(startDate: Date): String = {
    val endDate: Date = new Date()
    val elapsedTime: Long = (endDate.getTime - startDate.getTime) / 1000
    val minutes: Long = elapsedTime / 60
    val seconds: Long = elapsedTime % 60
    s"Processing time: ${minutes}min e ${seconds}s\n"
  }
}
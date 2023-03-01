import java.util.Date
import scala.util.{Failure, Success, Try}


case class InputParameters_CSV2Mongodb(csv: String,
                                       database: String,
                                       collection: String,
                                       host: Option[String],
                                       port: Option[Int],
                                       user: Option[String],
                                       password: Option[String],
                                       repeatSeparator: Option[String],
                                       convTable: Option[String],
                                       append: Boolean,
                                       noUpdDate: Boolean,
                                       hasHeader: Boolean)

class CSV2Mongodb {

  private def exportCSV2Mongodb(parameters: InputParameters_CSV2Mongodb): Try[Unit] = {
    Try {
      println(parameters)
    }
  }
}

object CSV2Mongodb {

  private def usage(): Unit = {
    System.err.println("\nEnter the input parameters:")
    System.err.println("-csv=<path>          ~ required     - Csv file directory")
    System.err.println("-database=<name>     ~ required     - MongoDB database name")
    System.err.println("-collection=<name>   ~ required     - MongoDB database collection name")
    System.err.println("[-host=<name>]                      - MongoDB server name. Default value is 'localhost'")
    System.err.println("[-port=<number>]                    - MongoDB server port number. Default value is 27017")
    System.err.println("[-user=<name>])                     - MongoDB user name")
    System.err.println("[-password=<pwd>]                   - MongoDB user password")
    System.err.println("[-repeatSeparator=<str>]            - String separating repetitive occurrences")
    System.err.println("[-convTable=<path>]                 - Conversion file path")
    System.err.println("[--append]                          - If absent, it will append documents into the MongoDB database, otherwise the database will be previously erased")
    System.err.println("[--noUpdDate]                       - If present, it will not create de update date field (_updd), otherwise it will be created")
    System.err.println("[--hasHeader]                       - If present the csv has a header\n")
    System.exit(1)
  }

  private def invalidParamWarning(param: String): Unit = {
    System.err.println(s"\n~ Parameter '$param' does not exist!")
    usage()
  }

  def main(args: Array[String]): Unit = {

    if (args.length < 3) usage()

    val parameters: Map[String, String] = args.foldLeft[Map[String, String]](Map()) {
      case (map, par) =>
        val split = par.split(" *= *", 2)
        if (split.size == 1) map + ((split(0).substring(2), ""))
        else map + (split(0).substring(1) -> split(1))
    }

    if (!Set("csv", "database", "collection").forall(parameters.contains)) usage()

    val listInputParam: List[String] = List("csv", "database", "collection", "host", "port", "user", "password",
      "repeatSeparator", "convTable", "append", "noUpdDate", "hasHeader")

    val paramsInvalid: List[String] = parameters.keys.dropWhile(listInputParam.contains).toList

    if (paramsInvalid.nonEmpty){
      invalidParamWarning(paramsInvalid.head)
    }

    val csv: String = parameters("csv")
    val database: String = parameters("database")
    val collection: String = parameters("collection")
    val host: Option[String] = parameters.get("host")
    val port: Option[Int] = parameters.get("port").flatMap(_.toIntOption)
    val user: Option[String] = parameters.get("user")
    val password: Option[String] = parameters.get("password")
    val repeatSeparator: Option[String] = parameters.get("repeatSeparator")
    val convTable: Option[String] = parameters.get("convTable")
    val append: Boolean = parameters.contains("append")
    val noUpdDate: Boolean = parameters.contains("noUpdDate")
    val hasHeader: Boolean = parameters.contains("hasHeader")

    val params: InputParameters_CSV2Mongodb = InputParameters_CSV2Mongodb(csv, database, collection, host, port,
      user, password, repeatSeparator, convTable, append, noUpdDate, hasHeader)
    val startDate: Date = new Date()

    (new CSV2Mongodb).exportCSV2Mongodb(params) match {
      case Success(_) =>
        println(timeAtProcessing(startDate))
        System.exit(0)
      case Failure(exception) =>
        println(exception.getMessage)
        System.exit(1)
    }
  }

  private def timeAtProcessing(startDate: Date): String = {
    val endDate: Date = new Date()
    val elapsedTime: Long = (endDate.getTime - startDate.getTime) / 1000
    val minutes: Long = elapsedTime / 60
    val seconds: Long = elapsedTime % 60
    s"Processing time: ${minutes}min e ${seconds}s\n"
  }
}
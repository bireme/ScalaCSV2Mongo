import mongodb.MongoExport
import org.bson.BsonValue
import org.json4s.DefaultFormats
import org.json4s.native.Json
import org.mongodb.scala.bson.{BsonArray, BsonString}
import org.mongodb.scala.bson.collection.immutable.Document

import java.util.Date
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

case class InputParameters_CompMongoCol(database_from1: String,
                                        collection_from1: String,
                                        collection_from2: String,
                                        collection_out: String,
                                        idField: String,
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
                                        noCompFields: Option[String],
                                        takeFields: Option[String],
                                        noUpDate: Boolean,
                                        append: Boolean)


class CompMongoCol {

  private def compMongoCol(parameters: InputParameters_CompMongoCol): Try[Unit] = {
    Try {
      val mongo_instance1: MongoExport = new MongoExport(parameters.database_from1, parameters.collection_from1, parameters.host_from1, parameters.port_from1, parameters.user_from1, parameters.password_from1, parameters.total, parameters.append)
      val mongo_instance2: MongoExport = new MongoExport(parameters.database_from2.getOrElse(""), parameters.collection_from2, parameters.host_from2, parameters.port_from2, parameters.user_from2, parameters.password_from2, parameters.total, parameters.append)
      val mongo_instanceOut: MongoExport = new MongoExport(parameters.database_out.getOrElse(""), parameters.collection_out, parameters.host_out, parameters.port_out, parameters.user_out, parameters.password_out, parameters.total, parameters.append)

      val docs_instance1: Seq[Document] = mongo_instance1.findAll
      val docs_instance2: Seq[Document] = mongo_instance2.findAll

      val documentsCompared: Seq[Array[(String, Array[BsonValue])]] = compareDocuments(docs_instance1, docs_instance2, parameters.idField, parameters.noCompFields, parameters.takeFields)
      //val documentsComparedSortedByKey = documentsCompared.sortBy(_.head._1).map(_.sortBy(_._1))
      val listJson: Seq[String] = documentsCompared.map(f => Json(DefaultFormats).write(f.toMap.map(f => getValue(f._1, f._2))))
      listJson.sorted.foreach(mongo_instanceOut.insertDocument)
    }
  }


  private def compareDocuments(docs_list1: Seq[Document], docs_list2: Seq[Document], identifierField: String, noCompFields: Option[String], takeFields: Option[String]):  Seq[Array[(String, Array[BsonValue])]] = {

    val noCompFieldsParam: Array[String] = noCompFields.getOrElse("_id").split(",") :+ "_id"
    val takeFieldsParam: Array[String] = takeFields.getOrElse(identifierField).split(",") :+ identifierField

    val docsListOneDiffDocsListTwo: Seq[Document] = compareDocumentsBetweenLists(docs_list1, docs_list2, takeFieldsParam)
    val docsListTwoDiffDocsListOne: Seq[Document] = compareDocumentsBetweenLists(docs_list2, docs_list1, takeFieldsParam)

    val docsListOneValid: Seq[Document] = deleteDocumentsWithUnnecessaryFields(docsListOneDiffDocsListTwo, identifierField)
    val listDocsOneWithoutMongoId: Seq[Document] = docsListOneValid.map(doc => doc.filterNot(field => noCompFieldsParam.contains(field._1)))

    val docsListTwoValid: Seq[Document] = deleteDocumentsWithUnnecessaryFields(docsListTwoDiffDocsListOne, identifierField)
    val listDocsTwoWithoutMongoId: Seq[Document] = docsListTwoValid.map(doc => doc.filterNot(field => noCompFieldsParam.contains(field._1)))

    val listDocsOneFieldsRename: Map[String, Document] = renameFieldsWith_InstanceNumber(listDocsOneWithoutMongoId, identifierField, "1").map(doc => (doc.getString(identifierField), doc)).toMap
    val listDocsOneAndTwoComparedUnified: Map[String, Document] = mergeFieldsInDocumentsById(identifierField, listDocsTwoWithoutMongoId, listDocsOneFieldsRename)

    val updatedList: Seq[Document] = listDocsOneAndTwoComparedUnified.values.toList

    defineArrayValuePosition(updatedList)
  }


  private def mergeFieldsInDocumentsById(identifierField: String, listDocsTwoWithoutMongoId: Seq[Document], listDocsOneFieldsRename: Map[String, Document]): Map[String, Document] = {
    renameFieldsWith_InstanceNumber(listDocsTwoWithoutMongoId, identifierField, "2").foldLeft(listDocsOneFieldsRename) { (acc, doc) =>
      acc.get(doc.getString(identifierField)) match {
        case Some(existingDoc) => acc.updated(doc.getString(identifierField), existingDoc ++ doc)
        case None => acc
      }
    }
  }

  private def deleteDocumentsWithUnnecessaryFields(docsListOneDiffDocsListTwo: Seq[Document], identifierField: String): Seq[Document] = {
    docsListOneDiffDocsListTwo.filterNot(f => f.isEmpty || f.contains("_id") && f.size == 1 || f.contains("_id") && f.contains(identifierField) && f.size <= 2)
  }

  private def compareDocumentsBetweenLists(list1: Seq[Document], list2: Seq[Document], takeFieldsParam: Array[String]): Seq[Document] = {
    list1.map(doc1 => doc1.filter{
      field1 =>
        if (!takeFieldsParam.contains(field1._1))
          !list2.exists(doc2 => doc2.exists(field2 => field1.equals(field2)))
        else true
    })
  }

  private def renameFieldsWith_InstanceNumber(list: Seq[Document], idField: String, numColl: String) : Seq[Document]= {

    val renamedList = list.map { originalDoc =>
      Document(
        originalDoc.map { case (oldKey, value) =>
          val newKey = oldKey match {
            case key if key == idField || key == "_id" => oldKey
            case _ => oldKey.concat(s" _$numColl")
          }
          newKey -> value
        }.toSeq: _*
      )
    }
    renamedList
  }

  private def defineArrayValuePosition(updatedList: Seq[Document]) : Seq[Array[(String, Array[BsonValue])]] = {

    updatedList.map(document => document.toArray.map(field => (field._1,
      if (field._1.endsWith(" _1")) {
        Array(field._2, if (field._2.isArray) BsonArray("") else BsonString(""))
      } else {
        Array(if (field._2.isArray) BsonArray("") else BsonString(""), field._2)
      }
    )))
  }


  private def getValue(key: String, value: Array[BsonValue]): (String, AnyRef) = {

    key match {
      case key if value.exists(_.isArray) => (key, value.map(_.asArray().getValues.asScala.map(_.asString().getValue)))
      case key if value.exists(_.isString) => (key, value.map(_.asString().getValue))
    }
  }
}

object CompMongoCol {

  private def usage(): Unit = {
    System.err.println("\nEnter the input parameters:")
    System.err.println("-database_from1=<name>                ~ required     - MongoDB database one name")
    System.err.println("-collection_from1=<name>              ~ required     - MongoDB database collection one name")
    System.err.println("-collection_from2=<name>              ~ required     - MongoDB database collection two name")
    System.err.println("-collection_out=<name>                ~ required     - MongoDB database collection out name")
    System.err.println("-idField=<str>                        ~ required     - Identifier field for document comparison")
    System.err.println("[-database_from2=<number>]            - MongoDB database two name")
    System.err.println("[-database_out=<name>])               - MongoDB database out name")
    System.err.println("[-host_from1=<pwd>]                   - MongoDB server name one. Default value is 'localhost'")
    System.err.println("[-port_from1=<str>]                   - MongoDB server port number one. Default value is 27017")
    System.err.println("[-host_from2=<path>]                  - MongoDB server name two. Default value is 'localhost'")
    System.err.println("[-port_from2=<str>]                   - MongoDB server port number two. Default value is 27017")
    System.err.println("[-host_out]                           - MongoDB server name out. Default value is 'localhost'")
    System.err.println("[-port_out]                           - MongoDB server port numbervout. Default value is 27017")
    System.err.println("[-user_from1]                         - MongoDB user name one")
    System.err.println("[-password_from1]                     - MongoDB user password one")
    System.err.println("[-user_from2]                         - MongoDB user name two")
    System.err.println("[-password_from2]                     - MongoDB user password two")
    System.err.println("[-user_out]                           - MongoDB user name out")
    System.err.println("[-password_out]                       - MongoDB user password out")
    System.err.println("[-total]                              - If present, take total documents")
    System.err.println("[-noCompFields]                       - Field(s) that will not be compared")
    System.err.println("[-takeFields]                         - Fields that must be present even if not being compared")
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

    val listInputParam: List[String] = List("database_from1", "collection_from1", "collection_from2", "collection_out", "idField", "database_from2", "database_out", "host_from1",
      "port_from1", "host_from2", "port_from2", "host_out", "port_out", "user_from1", "password_from1", "user_from2", "password_from2", "user_out", "password_out",
      "total", "noCompFields", "takeFields", "noUpdDate", "append")

    val paramsInvalid: List[String] = parameters.keys.dropWhile(listInputParam.contains).toList

    if (paramsInvalid.nonEmpty) {
      invalidParamWarning(paramsInvalid.head)
    }

    val database_from1: String = parameters("database_from1")
    val collection_from1: String = parameters("collection_from1")
    val collection_from2: String = parameters("collection_from2")
    val collection_out: String = parameters("collection_out")
    val idField: String = parameters("idField")
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
    val noCompFields: Option[String] = parameters.get("noCompFields")
    val takeFields: Option[String] = parameters.get("takeFields")
    val noUpDate: Boolean = parameters.contains("noUpDate")
    val append: Boolean = parameters.contains("append")

    val params: InputParameters_CompMongoCol = InputParameters_CompMongoCol(database_from1, collection_from1, collection_from2,
      collection_out, idField, database_from2, database_out, host_from1, port_from1, host_from2, port_from2, host_out, port_out,
      user_from1, password_from1, user_from2, password_from2, user_out, password_out, total, noCompFields, takeFields, noUpDate, append)
    val startDate: Date = new Date()
    (new CompMongoCol).compMongoCol(params) match {
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
package org.example

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession



object tasksexec extends App {

  val spark = SparkSession.builder().config("spark.master", "local")
    .appName("capg_questions")
    .getOrCreate()
//  val sc = spark.sparkContext

  //-----------------------------------------------
  // Spark configs and initial variables
  //-----------------------------------------------

  val configs: Config = ConfigFactory.load("properties.conf")
  import spark.implicits._

  // Logger
  val rootLogger = Logger.getRootLogger
  rootLogger.setLevel(Level.ERROR)

  //*************************************
  //SPARK SQL SOLUTION                  *
  //*************************************
  rootLogger.info("---Solution 1---")
  println("---Solution 1---")

  //Define Data structs
  case class moviesDb(film:String, genre:String, studio:String, score: Int, protability: Double,rtpercentage:Int, gross:String,year:Int )

  //Input data
  val data = spark.sparkContext.textFile(configs.getString("paths.q1data"))

  // File Header - column names
  val columnsHeader:String = data.take(1)(0)

  //Assign schema to remaining data
  val moviesInp = data.filter(row => row != columnsHeader)
    .map(_.split(","))
    .map(p => moviesDb(p(0).trim, p(1).trim, p(2).trim, p(3).trim.toInt,p(4).trim.toDouble,p(5).trim.toInt,p(6).trim,p(7).trim.toInt)).toDF()

  //Register as temp table
  moviesInp.createOrReplaceTempView("moviestable")

  //Query string
  var condition = configs.getString("queries.filter1ConditionString")
  val queryCustomFilter1:String = s"Select $columnsHeader from moviestable where $condition"

  //Result set
  val customFilter1 = spark.sql(s"$queryCustomFilter1")
  customFilter1.show()

  //*************************************
  //Word Count Solution                 *
  //*************************************
  rootLogger.info("---Solution 2---")
  println("---Solution 2---")
  val textFile = spark.sparkContext.textFile(configs.getString("paths.q2data"))
  val counts = textFile.flatMap(line => line.split(" "))
    .map(word => (word, 1))
    .reduceByKey(_ + _)
  counts.collect().foreach(x=>println(x))

}

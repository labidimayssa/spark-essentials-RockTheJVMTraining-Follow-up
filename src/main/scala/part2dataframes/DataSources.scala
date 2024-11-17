package part2dataframes

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}

object DataSources  extends App {

  val spark = SparkSession.builder()
    .appName("DataSources")
    .config("spark.master","local")
    .getOrCreate()


  val carsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", StringType),
    StructField("Origin", StringType)
  ))

  val carsDF = spark.read
    .format("json")
    .schema(carsSchema) //enforce a schema
/**  Important
 * mode = what sparks should do in case it encounters malformed data
  **/
    .option("mode","permessive") // mode : DROPMalformed : will ignore faulty rows, permessive (default)
    //Failfast will throw an exception if we encounter malformed record.
    .load("src/main/resources/data/cars.json")

  carsDF.show()
  //carsDFWithOptionMap.show() // /show is an action

  /*
  Writing DF
  - format
  - save mode = overwrite , append , ignore , errorIfExists
  - path
   */

  carsDF.write
    .mode(SaveMode.Append)
    .save("src/main/ressources/cars_write.json")

  // Reading from   a remote DB
  val employeesDF = spark.read
    .format("jdbc")
    .options(Map(
      "driver" -> "org.postgresql.Driver",
      "url" -> "jdbc:postgresql://localhost:5432/rtjvm",
      "user" -> "docker",
      "password" -> "docker",
      "dbtable" -> "public.employees"
    ))
    .load()

  //employeesDF.show()
  /**
   * Exercice : read the movies DF , then write it as
   * tab separated values file csv
   * snappy parquet
   * write movies DF in the postgre DB on a table called public.movies
   */

  val moviesDf = spark.read
    .format("json")
    .options(Map(
    "mode"-> "DropMalformed",
    "path"-> "src/main/resources/data/movies.json",
    "inferSchema"->"true"))
    .load()

  //moviesDf.show(3)

  moviesDf.write
    .mode(SaveMode.Overwrite)
    .option("header","true")
    .option("sep","\t")
    .csv("src/main/resources/data/movies.csv")


  moviesDf.write.mode(SaveMode.Ignore)
    .save("src/main/resources/data/movies.parquet")


  val driver = "org.postgresql.Driver"
  val url ="jdbc:postgresql://localhost:5432/rtjvm"
  val user = "docker"
  val password =  "docker"

  moviesDf.write
    .format("jdbc")
    .options(Map(
      "driver" -> driver,
      "url" -> url,
      "user" -> user,
      "password" ->password ))
    .mode(SaveMode.Ignore)
    .option("dbtable","public.movies")
    .save()



}

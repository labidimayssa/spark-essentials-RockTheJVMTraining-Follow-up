package part2dataframes

import org.apache.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.util.DropMalformedMode
import org.apache.spark.sql.functions.col

object ColumnsAndExpressions extends App {

  val spark = SparkSession.builder()
    .appName("DF Columns and Expressions")
    .config("spark.master", "local")
    .getOrCreate()


  /** Exercices
   * 1. Read movies DF and select 2 columns  of your choice
   * 2. sum 3 column Gross and put value on new column named total profit of the movies
   * 3. select all comedy movies Major_Genre withn imdb rating 6
   * use as many version as possible
   *
   */

  val moviesDF = spark.read
    .option("option", "DropMalformed")
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  //moviesDF.show()

  moviesDF.select("Title", "Running_Time_min")
  val moviesGross = moviesDF.withColumn("total_profit", col("Worldwide_Gross") + col("US_Gross") + col("US_DVD_Sales"))
  moviesGross.show(10)
  val genre = moviesDF.select("Major_Genre").distinct().show()

  val comedyMovies = moviesDF.filter(col("Major_Genre") === "Comedy" && col("IMDB_Rating") > 6.1)
  comedyMovies.show(5)

  val comedyMovies2 = moviesDF.filter("Major_Genre = 'Comedy' and IMDB_Rating > 6.1")
  comedyMovies2.show(5)


}
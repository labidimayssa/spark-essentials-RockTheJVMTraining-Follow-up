package part2dataframes

import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{approx_count_distinct, asc, avg, col, count, countDistinct, desc, desc_nulls_last, exp, lit, mean, stddev, sum}

object Aggregations extends App {

  var spark = SparkSession.builder()
    .appName("Aggregations AND Grouping")
    .config("spark.master", "local")
    .getOrCreate()

  //counting

  val moviesDB = spark.read
    .format("json")
    .option("mode", "DropMalformed")
    .option("inferSchema", "true")
    .load("src/main/resources/data/movies.json")
  moviesDB.show(10)


  moviesDB.select(count("*"))
  moviesDB.select(countDistinct(col("Major_Genre"))).show()

  //approximate counting : this function is very useful for Big Data Frames on which you want to do quick data analysis
  //approx count will not scan the entire dataFrame but rather it will get you an approximate row count
  moviesDB.select(approx_count_distinct(col("Major_Genre"))).show()

  //min
  val minRatingDF = moviesDB.select(functions.min(col("IMDB_Rating"))).show()
  //data Science
  //standard deviation means how close or or far the different values to the mean
  // a lower standard deviation will mean that the values are closer to the average and
  // the higher standard deviation will mean that the values are more spread out over a wider spectrum.

  moviesDB.select(mean(col("IMDB_Rating")), stddev(col("IMDB_Rating")))
    .show()

  //Grouping

  //count films by genre

  val countByGenre = moviesDB.groupBy(col("Major_genre"))
    .count().as("total")
  countByGenre.show()

  val aggregationDF = moviesDB.groupBy(col("Major_genre"))
    .agg(count("*").as("N_Movies"),
      avg("IMDB_Rating").as("Avg_IMDB_Rating"))
    .orderBy(desc("N_Movies"), desc("Avg_IMDB_Rating"))
    .show()

  /**
   * 1. Sum up all profits of all movies
   * 2. how many distinct directors we have in the DF
   * 3.mean and standard deviation of us gross revenue
   * avg imdb rating et the average US gross revenue per director
   */

  val profits = moviesDB.select((col("US_DVD_Sales") + col("US_Gross") + col("Worldwide_Gross")).as("total_Gross"))
    .agg(sum("total_Gross")).show()


  val distinctDirector = moviesDB.where(col("Director").isNotNull).select(col("Director")).distinct().count()
  println(s"we have $distinctDirector distinct directors  in the DF")

  moviesDB.select(countDistinct(col("Director"))).show()

  val statisticDf = moviesDB
    .agg(mean("US_Gross"),
      stddev("US_Gross")).show()

  val imdbDF = moviesDB.groupBy(col("Director"))
    .agg(avg(col("IMDB_Rating")).as("avg_IMDB_Rating"),
      avg(col("US_Gross")))
    .orderBy(desc_nulls_last("avg_IMDB_Rating"))
    .show()


}

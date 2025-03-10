package part5lowlevel

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.io.Source

object RDDs extends App {

  // Création de la SparkSession
  val spark = SparkSession.builder()
    .appName("Introduction to Low-Level Part RDD")
    .config("spark.master", "local")
    .getOrCreate()

  val sc = spark.sparkContext

  /** 1- Parallelize an existing collection */
  val numbers = 1 to 10000
  val numbersRDD = sc.parallelize(numbers)

  /** 2- Reading from files */
  case class StockValue(symbol: String, date: String, price: Double)

  def readStocks(filename: String): List[StockValue] = {
    Source.fromFile(filename)
      .getLines()
      .drop(1)
      .map(line => line.split(","))
      .map(tokens => StockValue(tokens(0), tokens(1), tokens(2).toDouble))
      .toList
  }

  val stockLines = readStocks("src/main/resources/data/stocks.csv")
  val stocksRDD = sc.parallelize(stockLines)

  // Using textFile and transformations
  val stocksRDD2 = sc.textFile("src/main/resources/data/stocks.csv")
    .map(row => row.split(","))
    .filter(tokens => tokens(0).toUpperCase == tokens(0)) // Filter to get uppercase symbols
    .map(tokens => StockValue(tokens(0), tokens(1), tokens(2).toDouble))

  println(s"Number of elements in stocksRDD2: ${stocksRDD2.count()}")

  /** 3- The easiest way is to read from a dataframe then Dataset  */
  val stocksDF = spark.read
    .option("delimiter", ",")
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/data/stocks.csv")

  // Convert DataFrame to Dataset and then to RDD
  import spark.implicits._
  val stocksDS = stocksDF.as[StockValue]
  val stocksRDD3 = stocksDS.rdd

  // RDD to DataFrame and Dataset conversion
  val numbersDF = numbersRDD.toDF("numbers") // Type information is lost here
  val numbersDS = numbersRDD.toDS() // Type information is retained here

  /** Working with RDDs */
  /** Transformations */

  /** Filter only Microsoft stocks (MSFT) */
  val msftRDD = stocksRDD.filter(line => line.symbol == "MSFT")
  msftRDD.count() // Action that triggers execution

  /** Distinct */
  // Use distinct to get company names without duplicates
  val companyNamesRDD = stocksRDD.map(_.symbol).distinct()
  companyNamesRDD.foreach(println)
  /** Min */
  // Using min() after defining an implicit Ordering
  implicit val orderingByPrice: Ordering[StockValue] = Ordering.by(_.price)
  val minMsft = msftRDD.min()
  println(minMsft)

  /** Reduce */
  // Use reduce to sum the numbers
  val total = numbersRDD.reduce(_ + _)
  println(total)

  /** Grouping */
  // Grouping stocks by symbol
  val groupedStocksRDD = stocksRDD.groupBy(_.symbol)
  groupedStocksRDD.foreach(println)


  /** Repartition */
  // Partition the RDDs
  val repartitionedStocksRDD = stocksRDD.repartition(30)
  repartitionedStocksRDD.toDF().write
    .mode(SaveMode.Overwrite)
    .parquet("src/main/resources/data/stocks30")

  // Coalescing to reduce the number of partitions without shuffle
  val coalescedRDD = repartitionedStocksRDD.coalesce(15)
  coalescedRDD.toDF().write
    .mode(SaveMode.Overwrite)
    .parquet("src/main/resources/data/stocks15")

  /** Exercises */

  case class Movie(title: String, genre: String, rating: Double)

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // Create an RDD of Movie from the DataFrame
  val moviesRDD = moviesDF.select(col("Title").as("title"), col("Major_Genre").as("genre"), col("IMDB_Rating").as("rating"))
    .filter(col("genre").isNotNull && col("IMDB_Rating").isNotNull)
    .as[Movie]
    .rdd

  // 2 - Show distinct genres
  moviesRDD.map(_.genre).distinct().foreach(println)

  // 3 - Select all movies in the Drama genre with IMDB rating > 6
  val goodDramaRDD = moviesRDD.filter(movie => movie.genre == "Drama" && movie.rating > 6)
  goodDramaRDD.toDF().show()

  // 4 - Show the average rating of movies by genre
  case class GenreAvgRating(genre: String, rating: Double)
  val avgRatingMoviesByGenreRDD = moviesRDD.groupBy(_.genre).map {
    case (genre, movies) => GenreAvgRating(genre, movies.map(_.rating).sum / movies.size)
  }

  avgRatingMoviesByGenreRDD.toDF().show()

  /** Takeaways */
  /*
   * Collection transformations: map, flatMap, filter
   * Actions: count, min, reduce
   * stocksRDD.min !! needs implicit ordering
   * Grouping: stocksRDD.groupBy(_.symbol) !! involves shuffling
   * Partitioning: stocksRDD.repartition(40) !! involves shuffling
   * : repartitionedStocksRDD.coalesce(10) is not a full shuffle
   */
}

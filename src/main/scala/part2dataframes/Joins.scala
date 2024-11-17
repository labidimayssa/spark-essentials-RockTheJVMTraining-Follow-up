package part2dataframes

import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.spark.sql.functions.{array_contains, asc, col, desc, desc_nulls_last, expr, first}


object Joins extends App {


  var spark = SparkSession.builder()
    .appName("Joins Exercies")
    .config("spark.master", "local")
    .getOrCreate()


  val guitarsDf = spark.read.format("json")
    .option("inferSchema", "true")
    .option("dropMalformed", "true")
    .load("src/main/resources/data/guitars.json")

  guitarsDf.show(10)

  val guitaristsDf = spark.read.format("json")
    .option("inferSchema", "true")
    .option("dropMalformed", "true")
    .load("src/main/resources/data/guitarPlayers.json")
  guitaristsDf.show(10)


  val bandsDf = spark.read.format("json")
    .option("inferSchema", "true")
    .option("dropMalformed", "true")
    .load("src/main/resources/data/bands.json")

  bandsDf.show(10)

  /** Join Types : */
  val JoinExpression = bandsDf.col("id") === guitaristsDf.col("band")
  //inner join
  val guitarsBandsDF = guitaristsDf.join(bandsDf, JoinExpression, "inner")
  guitarsBandsDF.show()


  // left outer = everything in the inner join + all the rows in the LEFT Table, with nulls in where data is missing
  guitaristsDf.join(bandsDf,JoinExpression, "left_outer" ).show()

  // right outer = everything in the inner join + all the rows in the RIGHT Table, with nulls in where data is missing
  guitaristsDf.join(bandsDf,JoinExpression, "right_outer" ).show()

  // outer join =  everything in the inner join + all the rows in the BOTH Tables, with nulls in where data is missing
  guitaristsDf.join(bandsDf,JoinExpression, "outer" ).show()

  // semi join = everything in the left DF for which there is a row in the right DF satisfying the condition
  guitaristsDf.join(bandsDf,JoinExpression, "left_semi" ).show()

  // anti join = everything in the left DF for which there is NO row in the right DF satisfying the condition
  guitaristsDf.join(bandsDf,JoinExpression, "left_anti" ).show()

  // Join using array
  guitaristsDf.join(guitarsDf.withColumnRenamed("id", "guitar_id"), expr("array_contains(guitars,guitar_id)"), "left")
    .show()

  /**
   *   Exercises
   *   1. show all employees and their max salaries
   *   2. show all employees who were never managers
   *   3. find all the job titles of the best paid 10 employees in the company
   */

  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/rtjvm"
  val user = "docker"
  val password = "docker"
  val emloyeesTable = "employees"
  val salariesTable = "salaries"
  val departmentsTable = "departments"
  val dept_empTable = "dept_emp"
  val deptManagerTable = "dept_manager"
  val jobTitleTable = "titles"


  def readTable(tableName: String): DataFrame = {
    val dataFrameDB = spark.read
      .format("jdbc")
      .options(Map(
        "drive" -> driver,
        "url" -> url,
        "user" -> user,
        "password" -> password,
        "dbtable" -> tableName
      )).load()

    dataFrameDB
  }

  val emloyeesDf = readTable(emloyeesTable)
  emloyeesDf.show(5)

  val salariesDf = readTable(salariesTable)
  salariesDf.show(5)

  val deptManagerDf = readTable(deptManagerTable)
  deptManagerDf.show(5)

  val joinempnoKey = Seq("emp_no")
  val emloyeesSalariesDf = emloyeesDf.join(salariesDf, joinempnoKey, "left")
  emloyeesSalariesDf.show(5)

  val maxSalaryByEmp = emloyeesSalariesDf.select(col("emp_no"), col("first_name"), col("last_name"), col("Salary"))
    .groupBy(col("emp_no"), col("first_name")).agg(functions.max(col("salary")).as("max_salary"))
    .orderBy(desc_nulls_last("max_salary"))
  maxSalaryByEmp.show(10)


  val noManagerEmployeesDf = emloyeesDf.join(deptManagerDf, joinempnoKey, "left")
     .filter(col("dept_no").isNull)
  println("employees who were never managers",noManagerEmployeesDf.count())

  val noManagerEmployeesDf2 = emloyeesDf.join(deptManagerDf, joinempnoKey, "left_anti")
  println("employees who were never managers with second solution",noManagerEmployeesDf2.count())


  val jobTitleDf = readTable(jobTitleTable)
  val RecentjobTitleDf = jobTitleDf.groupBy(col("emp_no"))
    .agg(functions.max("to_date"), first("title"))
  RecentjobTitleDf.show()


  val top10PaidTitleJobDF =  maxSalaryByEmp.join(RecentjobTitleDf, joinempnoKey, "left")
  top10PaidTitleJobDF.show()

}

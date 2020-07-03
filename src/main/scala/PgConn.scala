import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

class PgConn {
  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)
  val spark = SparkSession
    .builder()
    .appName("Spark Postgres basic example")
    .master("local[*]")
    .getOrCreate()

  def JdbcExample(): Unit = {
    val jdbcDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://localhost/testdb")
      .option("dbtable", "employee")
      .option("user", "scala")
      .option("password", "scala")
      .load()
        .orderBy("id")
    jdbcDF.show()


    jdbcDF.write
      .format("csv")
      .option("header","true")
      .option("delimiter","\t")
      .save("/home/sajita/hey")

  }
}

object JdbcConn {
  def main(args: Array[String]): Unit = {
    val DBconnObject = new PgConn
    DBconnObject.JdbcExample()
  }
}
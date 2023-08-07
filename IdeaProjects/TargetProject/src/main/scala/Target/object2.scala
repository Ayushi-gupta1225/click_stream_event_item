package Target

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object object2 {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("object2").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    val folderPath = "C:\\Users\\Hp\\OneDrive\\Desktop\\PipeLineData\\clickstream_log (1).csv"

    // Read all CSV files from the folder into a DataFrame
    val df: DataFrame = spark.read
      .option("header", "true") // If the CSV files have a header row, set this option to "true"
      .option("inferSchema", "true") // Infers the data types of columns
      .csv(folderPath)
df.show()
    spark.stop()
  }
}

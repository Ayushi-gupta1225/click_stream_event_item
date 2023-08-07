package Target
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object Target {
  def main(args: Array[String]): Unit = {
    val config = ConfigFactory.load("application.conf")
    val sparkMaster = config.getString("spark.master")
    val sparkConf = new SparkConf().setAppName("ClickstreamDataPipeline").setMaster(sparkMaster)
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()


//    val sparkConf = new SparkConf().setAppName("ClickstreamDataPipeline")
//    val spark = SparkSession.builder().config(sparkConf).getOrCreate().setMaster(sparkMaster)
    //    val spark = SparkSession.builder()
//      .master("local")
//      .appName("ClickstreamDataPipeline")
//      .getOrCreate()
//    System.setProperty("hadoop.home.dir", "C:\\hadoop-2.7.1")
//    spark.sparkContext.setLogLevel("error")


    val inputPath1 = config.getString("input.path1")
    val inputPath2 = config.getString("input.path2")
    val outputPath=config.getString("output.path")

    //    val folderPath1 = "C:\\Users\\Hp\\OneDrive\\Desktop\\PipeLineData\\clickstream_log (1).csv"
//    val folderPath2 = "C:\\Users\\Hp\\OneDrive\\Desktop\\PipeLineData\\item_data (1).csv"
    // Read all CSV files from the folder into a DataFrame

    val df1: DataFrame = spark.read
      .option("header", "true") // If the CSV files have a header row, set this option to "true"
      .option("inferSchema", "true") // Infers the data types of columns
      .csv(inputPath1)
    val df2: DataFrame = spark.read
      .option("header", "true") // If the CSV files have a header row, set this option to "true"
      .option("inferSchema", "true") // Infers the data types of columns
      .csv(inputPath2)

    val df1WithoutDuplicates = df1.dropDuplicates("id")//-------drop duplicates from id column
    val df2WithoutDuplicates = df2.dropDuplicates("item_id")

   val df1cast= df1WithoutDuplicates.withColumn("event_timestamp",
      to_timestamp(col("event_timestamp")))
    //val df1cast= df1WithoutDuplicates.withColumn("event_timestamp", to_timestamp($"date_string", "MM/dd/yy HH:mm"))

    val df2cast= df2WithoutDuplicates.withColumn("item_price",col("item_price").cast("Double"))

     val df1rename=df1cast.select(col("id").as("Entity_id"),col("event_timestamp"),
      col("device_type").as("device_type_t"),
      col("session_id").as("visitor_session_c"),col("visitor_id"),col("item_id"),
      col("redirection_source").as("redirection_source_t") )
    val df2rename=df2cast.select(col("item_id"),
      col("item_price").as("item_unit_price_a"),
      col("product_type").as("product_type_c"),
      col("department_name").as("department_n")).printSchema()

    df1rename.write.option("header","true").csv(outputPath)

    //
//
    spark.stop()
  }
}

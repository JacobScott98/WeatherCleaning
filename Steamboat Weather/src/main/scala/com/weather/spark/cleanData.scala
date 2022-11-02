package com.weather.spark

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when, substring, avg}


/** Cleaning up the Steamboat Weather Data for further analysis */
object cleanData {

  case class weather(Date: String, Max: String, Min: String, Precip: String, Snow: String)

  def main(args: Array[String]) {

    /** Setting the log level to errors only */
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Starting a Spark Session
    val spark = SparkSession
      .builder()
      .appName("SparkSQL")
      .master("local[*]")
      .getOrCreate()

    // Import the csv as a dataset
    import spark.implicits._
    val data = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/SteamboatSprings.csv")
      .as[weather]

    // Breaking up the Date into Year and Month Columns
    val data_dates = data.select(substring(col("Date"),0,4).cast("int").alias("Year"),substring(col("Date"),6,2).cast("int").alias("Month"),col("Max"),col("Min"),col("Precip"),col("Snow"))

    // Removing the years where data is entirely missing
    val dataFiltered = data_dates.filter(data_dates("Year") > "1908")

    // Checking to try and figure out what the "T" values mean in the snow category
    dataFiltered.filter(col("Snow") === "T").show() // Looks like all of these values occur during summer months, going to assume these can be changed to zeros

    // Replacing the T's with 0's
    val removeT = dataFiltered.withColumn("Snow", when(dataFiltered("Snow") === "T", "0").otherwise(dataFiltered("Snow")))
      .select("Year", "Month", "Max", "Min","Precip", "Snow")

    // Next I wanted to remove the missing "M" values and replace them with the average value for that month across the entire dataset. 
    // I know this isn't the most sophisticated method, but I mainly wanted to test my ability using basic Spark SQL functions
    
    // Removing M's and casting types to Integers to get average values
    val data4avg = removeT.filter(removeT("Max") =!= "M" && removeT("Min") =!= "M"  && removeT("Precip") =!= "M" && removeT("Snow") =!= "M")
      .select(col("Year"),col("Month"),col("Max").cast("int"),col("Min").cast("int"),col("Precip").cast("int"),col("Snow").cast("int"))

    // Setting up a dataset of average values grouped by month to use later
    val avg_values = data4avg.groupBy("Month").agg(avg("Max"),avg("Min"),avg("Precip"),avg("Snow"))
      .sort("Month")

    // Replacing "M" values with the average for that month

    // Joining average values onto the cleaned data, so we can replace the M's
    val avg_joined = removeT.join(avg_values,data4avg("Month") === avg_values("Month"),"left").sort(data4avg("Year"))

    // Replacing 'M' or missing values with the average for that month
    val removeM = avg_joined.withColumn("Max", when(avg_joined("Max")==='M',avg_joined("avg(Max)")).otherwise(avg_joined("Max")))
      .withColumn("Min", when(avg_joined("Min")==='M',avg_joined("avg(Min)")).otherwise(avg_joined("Min")))
      .withColumn("Precip", when(avg_joined("Precip")==='M',avg_joined("avg(Precip)")).otherwise(avg_joined("Precip")))
      .withColumn("Snow", when(avg_joined("Snow")==='M',avg_joined("avg(Snow)")).otherwise(avg_joined("Snow")))
      .select(col("Year"), removeT("Month"), col("Max"), col("Min"), col("Precip"), col("Snow"))
      .sort("Year", "Month")

    removeM.show()

    //Output to a csv file for analysis
    removeM.coalesce(1)
      .write
      .option("header", true)
      .csv("/data/CleanedSteamboatWeather")
    
  }
}

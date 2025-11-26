import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * CareemBatchIngest
 *
 * 1) Reads the UAE trips CSV (200k rows) from HDFS
 * 2) Cleans basic columns and adds a trip_date column
 * 3) Writes Silver layer (parquet, partitioned by city + trip_date)
 * 4) Writes Gold layer daily aggregates per city
 *
 * Adjust the HDFS input path if your file is in a different directory.
 */
object CareemBatchIngest {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Careem Batch Ingest - UAE Trips 200k")
      .getOrCreate()

    import spark.implicits._

    // ====== 1. Read raw CSV from HDFS (Bronze) ======
    // Example path: hdfs:///user/bigdata/careem/raw/uae_trips_200k.csv
    val inputPath = "hdfs:///user/bigdata/careem/raw/uae_trips_200k.csv"

    val tripsRaw = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(inputPath)

    // ====== 2. Basic cleaning + derived columns ======
    val tripsClean = tripsRaw
      // make sure timestamps are proper TimestampType
      .withColumn("pickup_ts", to_timestamp($"pickup_time"))
      .withColumn("dropoff_ts", to_timestamp($"dropoff_time"))
      // derive trip_date from pickup time
      .withColumn("trip_date", to_date($"pickup_ts"))
      // ensure city and service_type are trimmed
      .withColumn("city", trim($"city"))
      .withColumn("service_type", trim($"service_type"))
      // optional: filter out obvious bad records (nulls)
      .filter($"trip_id".isNotNull && $"pickup_ts".isNotNull && $"dropoff_ts".isNotNull)

    // ====== 3. Write Silver layer (parquet, partitioned) ======
    val silverPath = "hdfs:///user/bigdata/careem/silver/trips"

    tripsClean.write
      .mode("overwrite")
      .partitionBy("city", "trip_date")
      .parquet(silverPath)

    // ====== 4. Create Gold aggregates ======
    // Example: daily metrics per city
    val dailyAgg = tripsClean
      .groupBy($"city", $"trip_date")
      .agg(
        count("*").alias("trip_count"),
        sum($"fare_aed").alias("total_fare_aed"),
        avg($"fare_aed").alias("avg_fare_aed"),
        avg($"distance_km").alias("avg_distance_km")
      )
      .orderBy($"trip_date", $"city")

    val goldDailyPath = "hdfs:///user/bigdata/careem/gold/trips_daily_city"

    dailyAgg.write
      .mode("overwrite")
      .partitionBy("city")
      .parquet(goldDailyPath)

    spark.stop()
  }
}

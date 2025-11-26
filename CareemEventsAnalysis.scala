import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * CareemEventsAnalysis
 *
 * This job works on the 80k trip events CSV (uae_trip_events_stream_80k.csv)
 * as a normal batch DataFrame (NOT Structured Streaming) to keep it simple.
 *
 * It:
 * 1) Reads the events CSV from HDFS
 * 2) Parses timestamps and cleans city/area
 * 3) Computes simple aggregates such as:
 *    - count of events by city and event_type
 *    - top areas with most REQUESTED events
 * 4) Writes the results to the GOLD layer in HDFS.
 *
 * Use this as your second .scala file that uses the 80k file.
 */
object CareemEventsAnalysis {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Careem Events Analysis - UAE Trip Events 80k")
      .getOrCreate()

    import spark.implicits._

    // ====== 1. Read events CSV ======
    val eventsPath = "hdfs:///user/bigdata/careem/raw/uae_trip_events_stream_80k.csv"

    val eventsRaw = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(eventsPath)

    // ====== 2. Basic cleaning ======
    val eventsClean = eventsRaw
      .withColumn("event_time_ts", to_timestamp($"event_time"))
      .withColumn("event_date", to_date($"event_time_ts"))
      .withColumn("city", trim($"city"))
      .withColumn("area", trim($"area"))
      .withColumn("event_type", trim($"event_type"))
      .filter($"event_id".isNotNull && $"event_time_ts".isNotNull)

    // ====== 3a. Aggregate: events by city and type ======
    val eventsByCityAndType = eventsClean
      .groupBy($"city", $"event_type")
      .agg(count("*").alias("event_count"))
      .orderBy($"city", $"event_type")

    val eventsByCityAndTypePath =
      "hdfs:///user/bigdata/careem/gold/events_by_city_and_type"

    eventsByCityAndType.write
      .mode("overwrite")
      .partitionBy("city")
      .parquet(eventsByCityAndTypePath)

    // ====== 3b. Aggregate: top areas for REQUESTED events ======
    val requestedByArea = eventsClean
      .filter($"event_type" === "REQUESTED")
      .groupBy($"city", $"area")
      .agg(count("*").alias("request_count"))
      .orderBy($"request_count".desc)

    val requestedByAreaPath =
      "hdfs:///user/bigdata/careem/gold/requested_events_by_area"

    requestedByArea.write
      .mode("overwrite")
      .partitionBy("city")
      .parquet(requestedByAreaPath)

    spark.stop()
  }
}

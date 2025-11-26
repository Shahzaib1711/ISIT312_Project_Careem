import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * CareemBonusQueries
 *
 * OPTIONAL / BONUS:
 * This job reads the Silver and/or Gold tables and runs some extra analytics
 * that you can show in your report or screenshots.
 *
 * Examples:
 *  - Find the top 5 cities by total revenue
 *  - Find the busiest drivers (by number of trips)
 *  - Find the hour-of-day distribution of trips in Dubai
 *
 * You can run this after you have already executed CareemBatchIngest
 * so that the Silver/Gold data exists in HDFS.
 */
object CareemBonusQueries {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Careem Bonus Queries")
      .getOrCreate()

    import spark.implicits._

    // ====== 1. Read Silver trips table ======
    val silverTripsPath = "hdfs:///user/bigdata/careem/silver/trips"

    val trips = spark.read
      .parquet(silverTripsPath)

    trips.createOrReplaceTempView("trips")

    // ====== 2. Top 5 cities by total revenue ======
    val topCities = spark.sql(
      """SELECT city,
                 SUM(fare_aed) AS total_revenue_aed,
                 COUNT(*)      AS trip_count
          FROM trips
          GROUP BY city
          ORDER BY total_revenue_aed DESC
          LIMIT 5
      """)

    topCities.show(false)

    // ====== 3. Busiest drivers (by trips completed) ======
    val busiestDrivers = spark.sql(
      """SELECT driver_id,
                 city,
                 COUNT(*) AS trips_completed
          FROM trips
          WHERE status = 'COMPLETED'
          GROUP BY driver_id, city
          ORDER BY trips_completed DESC
          LIMIT 10
      """)

    busiestDrivers.show(false)

    // ====== 4. Hour-of-day distribution for Dubai ======
    val dubaiHourly = trips
      .filter($"city" === "Dubai")
      .withColumn("hour_of_day", hour($"pickup_ts"))
      .groupBy($"hour_of_day")
      .agg(count("*").alias("trip_count"))
      .orderBy($"hour_of_day")

    dubaiHourly.show(false)

    spark.stop()
  }
}

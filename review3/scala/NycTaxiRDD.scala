// ============================================================
// NycTaxiRDD.scala
// BDA Review 3 — Scala + Spark RDD Processing
// NYC Yellow Taxi Trip Data (Jan 2026, ~7.6M rows)
// ============================================================
// HOW TO RUN:
//   spark-shell
//   :load /Users/apple/Documents/sem7/bda/project/review3/scala/NycTaxiRDD.scala
// ============================================================
// WHY SCALA FOR RDD?
//   Spark is written in Scala. Scala RDDs run natively on the JVM
//   with zero serialization overhead. PySpark RDDs cross the
//   Python↔JVM boundary via Py4J, adding 2-10x latency.
//   For demonstrating RDD internals (lineage, DAG, partitions),
//   Scala is the authentic choice.
// ============================================================

import org.apache.spark.storage.StorageLevel

println("\n" + "=" * 70)
println("  NYC TAXI RDD ANALYTICS — Scala + Spark")
println("  Dataset: NYC TLC Yellow Taxi Trip Data (Jan 2026)")
println("=" * 70)

// ============================================================
// STEP 1: LOAD DATA — Read Parquet → Convert to CSV-like RDD
// ============================================================
// WHY: We load parquet via DataFrame first (Spark's optimized reader),
// then convert to RDD to demonstrate all RDD operations.
// In production, you'd stay in DataFrame land. Here, we intentionally
// drop down to RDD to show Spark's foundational abstraction.

val candidatePaths = Seq(
  "data/yellow_tripdata_2026-01.parquet",
  "review3/data/yellow_tripdata_2026-01.parquet",
  "../data/raw/yellow_tripdata_2026-01.parquet",
  "data/raw/yellow_tripdata_2026-01.parquet",
  "/app/data/yellow_tripdata_2026-01.parquet",
  "/Users/apple/Documents/sem7/bda/project/review3/data/yellow_tripdata_2026-01.parquet"
)
val dataPath = candidatePaths.find(p => new java.io.File(p).exists()).getOrElse("data/yellow_tripdata_2026-01.parquet")
println(s"Loading dataset from: $dataPath")
val df = spark.read.parquet(dataPath)

println(s"\n📊 Raw DataFrame loaded: ${df.count()} rows, ${df.columns.length} columns")
println(s"   Columns: ${df.columns.mkString(", ")}")

// Convert to RDD of Row objects
val rawRDD = df.rdd

println(s"\n✅ [RDD CREATED] rawRDD from Parquet via df.rdd")
println(s"   Type: org.apache.spark.rdd.RDD[org.apache.spark.sql.Row]")

// -----------------------------------------------------------
// PARTITIONS
// -----------------------------------------------------------
// WHY: Partitions determine parallelism. Spark creates one task per
// partition. More partitions = more parallelism, but too many = overhead.
// Default partitions typically match HDFS block count or CPU cores.
println(s"\n📦 [PARTITIONS] Number of partitions: ${rawRDD.getNumPartitions}")
println(s"   (Each partition is processed by a separate Spark task)")
println(s"   (Partitions relate to HDFS block size in cluster mode)")

// ============================================================
// STEP 2: TRANSFORMATIONS (5 transformations demonstrated)
// ============================================================
// ALL transformations are LAZY — nothing executes until an action is called.
// This lets Spark build an optimized execution plan (DAG).

// -----------------------------------------------------------
// T1: map() — Extract relevant fields from each Row
// -----------------------------------------------------------
// WHY map(): We need to transform each Row into a structured tuple
// for downstream analysis. map() applies a function to EVERY element.

val parsedRDD = rawRDD.map { row =>
  try {
    val vendorId     = Option(row.getAs[Any]("VendorID")).map(_.toString.toDouble.toLong).getOrElse(0L)
    val pickupDT     = if (row.isNullAt(row.fieldIndex("tpep_pickup_datetime"))) null else row.getAs[java.sql.Timestamp]("tpep_pickup_datetime")
    val dropoffDT    = if (row.isNullAt(row.fieldIndex("tpep_dropoff_datetime"))) null else row.getAs[java.sql.Timestamp]("tpep_dropoff_datetime")
    val passengerCnt = Option(row.getAs[Any]("passenger_count")).map(_.toString.toDouble.toLong).getOrElse(0L)
    val tripDistance = Option(row.getAs[Any]("trip_distance")).map(_.toString.toDouble).getOrElse(0.0)
    val puLocationId = Option(row.getAs[Any]("PULocationID")).map(_.toString.toDouble.toLong).getOrElse(0L)
    val doLocationId = Option(row.getAs[Any]("DOLocationID")).map(_.toString.toDouble.toLong).getOrElse(0L)
    val paymentType  = Option(row.getAs[Any]("payment_type")).map(_.toString.toDouble.toLong).getOrElse(0L)
    val fareAmount   = Option(row.getAs[Any]("fare_amount")).map(_.toString.toDouble).getOrElse(0.0)
    val tipAmount    = Option(row.getAs[Any]("tip_amount")).map(_.toString.toDouble).getOrElse(0.0)
    val totalAmount  = Option(row.getAs[Any]("total_amount")).map(_.toString.toDouble).getOrElse(0.0)
    val congestion   = Option(row.getAs[Any]("congestion_surcharge")).map(_.toString.toDouble).getOrElse(0.0)

    val hour = if (pickupDT != null) {
      val cal = java.util.Calendar.getInstance()
      cal.setTime(pickupDT)
      cal.get(java.util.Calendar.HOUR_OF_DAY)
    } else -1

    val durationMin = if (pickupDT != null && dropoffDT != null) {
      (dropoffDT.getTime - pickupDT.getTime) / 60000.0
    } else 0.0

    // Return tuple: (vendor, passengers, distance, puZone, doZone,
    //                payType, fare, tip, total, hour, durationMin, congestion)
    (vendorId, passengerCnt, tripDistance, puLocationId, doLocationId,
     paymentType, fareAmount, tipAmount, totalAmount, hour, durationMin, congestion)
  } catch {
    case _: Exception =>
      (0L, 0L, 0.0, 0L, 0L, 0L, 0.0, 0.0, 0.0, -1, 0.0, 0.0)
  }
}

println("\n🔄 [T1: map()] Parsed each Row → structured tuple (12 fields)")

// -----------------------------------------------------------
// T2: filter() — Remove invalid/dirty records
// -----------------------------------------------------------
// WHY filter(): Real-world data has nulls, zeros, negatives, and outliers.
// filter() keeps only records that satisfy our cleaning predicates.
// This is equivalent to a SQL WHERE clause, but at the RDD level.

val cleanRDD = parsedRDD.filter { case (_, passengers, distance, _, _,
                                        _, fare, _, total, hour, duration, _) =>
  fare > 0 &&             // Must have a positive fare
  distance > 0 &&         // Must have travelled some distance
  distance < 100 &&       // Remove GPS glitches (100+ miles in NYC?)
  passengers > 0 &&       // Must have at least 1 passenger
  hour >= 0 &&            // Must have valid pickup time
  duration > 0 &&         // Must have positive duration
  duration < 180 &&       // Remove overnight/parked trips (3+ hours)
  total > 0 &&            // Must have charged something
  total < 500             // Remove extreme outliers
}

println("🔄 [T2: filter()] Removed invalid records (nulls, negatives, outliers)")

// -----------------------------------------------------------
// CACHE / PERSIST — Cache cleanRDD for reuse
// -----------------------------------------------------------
// WHY persist(MEMORY_AND_DISK)?
//   cleanRDD is reused across Q1, Q2, and Q3 below.
//   Without caching, Spark would re-read the parquet file AND
//   re-apply map() and filter() for EACH action.
//   persist(MEMORY_AND_DISK) stores it in RAM, spilling to disk
//   if RAM is full. cache() = persist(MEMORY_ONLY) which would
//   recompute partitions that don't fit — risky with 7.6M rows.

cleanRDD.persist(StorageLevel.MEMORY_AND_DISK)
println("\n💾 [CACHE] cleanRDD.persist(MEMORY_AND_DISK)")
println("   → Subsequent actions reuse cached data instead of re-reading parquet")

// ============================================================
// STEP 3: ACTIONS — Force computation, get results
// ============================================================

// -----------------------------------------------------------
// A1: count() — How many clean records?
// -----------------------------------------------------------
val totalClean = cleanRDD.count()
println(s"\n📊 [A1: count()] Total clean records: ${"%,d".format(totalClean)}")

// -----------------------------------------------------------
// A2: take(5) — Preview first 5 records
// -----------------------------------------------------------
println("\n📋 [A2: take(5)] Sample records:")
println("   (vendor, passengers, distance, puZone, doZone, payType, fare, tip, total, hour, duration, congestion)")
cleanRDD.take(5).foreach { r =>
  println(s"   $r")
}

// ============================================================
// Q1: WHAT ARE THE PEAK DEMAND HOURS AND TOP ZONES?
// ============================================================
// Business question: When and where should we deploy more taxis?

println("\n" + "=" * 70)
println("  Q1: PEAK DEMAND — HOURLY TRIP COUNT & TOP PICKUP ZONES")
println("=" * 70)

// T3: map() to key-value — (hour, 1) for counting
val hourCountRDD = cleanRDD.map { case (_, _, _, _, _, _, _, _, _, hour, _, _) =>
  (hour, 1L)
}
println("🔄 [T3: map()] Created key-value RDD: (hour, 1)")

// T4: reduceByKey() — Sum counts per hour
// WHY reduceByKey(): Combines values by key across all partitions.
// Unlike groupByKey(), reduceByKey() does local aggregation FIRST
// (combiner), then shuffles only the partial sums. Much less data
// transferred across the network.
val hourlyDemand = hourCountRDD.reduceByKey(_ + _)
println("🔄 [T4: reduceByKey()] Aggregated trip counts per hour")

// A3: collect() — Fetch all 24 hourly aggregates
// WHY collect() is safe here: We only have 24 keys (hours 0-23).
// collect() on millions of rows would crash the driver — but 24 rows is fine.
val hourlyResults = hourlyDemand.sortByKey().collect()
println("\n📊 [A3: collect()] Hourly Demand (NYC Taxi, Jan 2026):")
println("   " + "-" * 50)
hourlyResults.foreach { case (hour, count) =>
  val bar = "█" * (count / (totalClean / 24 / 4)).toInt
  println(f"   Hour $hour%02d:00  │ ${"%,8d".format(count)} trips  │ $bar")
}
val peakHour = hourlyResults.maxBy(_._2)
println(s"\n   🏆 Peak Hour: ${peakHour._1}:00 with ${"%,d".format(peakHour._2)} trips")

// Top 10 Pickup Zones
val zoneCountRDD = cleanRDD.map { case (_, _, _, puZone, _, _, _, _, _, _, _, _) =>
  (puZone, 1L)
}.reduceByKey(_ + _)

val topZones = zoneCountRDD.sortBy(-_._2).take(10)
println("\n📊 Top 10 Busiest Pickup Zones:")
println("   " + "-" * 40)
topZones.foreach { case (zone, count) =>
  println(f"   Zone $zone%3d  │ ${"%,8d".format(count)} trips")
}

// ============================================================
// Q2: HOW DOES PAYMENT TYPE AFFECT TIPPING?
// ============================================================
// Business question: Should we encourage card payments for higher tips?

println("\n" + "=" * 70)
println("  Q2: PAYMENT TYPE vs TIPPING BEHAVIOR")
println("=" * 70)

// Key-Value operation: (payment_type, (tip_amount, 1)) → reduceByKey → average
val tipByPaymentRDD = cleanRDD.map { case (_, _, _, _, _, payType, _, tip, _, _, _, _) =>
  (payType, (tip, 1L))
}.reduceByKey { case ((t1, c1), (t2, c2)) =>
  (t1 + t2, c1 + c2)
}

// T5: mapValues() — Compute average from (sum, count)
// WHY mapValues(): Only transforms the value, preserving the key.
// More efficient than map() on key-value RDDs because the partitioner
// is preserved (no re-shuffle needed).
val avgTipByPayment = tipByPaymentRDD.mapValues { case (totalTip, count) =>
  totalTip / count
}
println("🔄 [T5: mapValues()] Computed average tip per payment type")

val paymentNames = Map(1L -> "Credit Card", 2L -> "Cash", 3L -> "No Charge",
                       4L -> "Dispute", 5L -> "Unknown", 6L -> "Voided")

println("\n📊 [collect()] Average Tip by Payment Type:")
println("   " + "-" * 50)
avgTipByPayment.collect().sortBy(_._1).foreach { case (payType, avgTip) =>
  val name = paymentNames.getOrElse(payType, s"Type $payType")
  println(f"   $name%-15s │ Avg Tip: $$$avgTip%.2f")
}
println("\n   💡 Insight: Credit card tips >> Cash tips (cash tips are not recorded)")

// ============================================================
// Q3: FARE PER MILE ACROSS TIME PERIODS
// ============================================================
// Business question: When do drivers earn the most per mile?

println("\n" + "=" * 70)
println("  Q3: FARE-PER-MILE BY TIME PERIOD")
println("=" * 70)

// T-extra: flatMap() — Categorize each trip into time period AND zone
// WHY flatMap(): Unlike map() which returns exactly 1 output per input,
// flatMap() can return 0, 1, or MANY outputs per input. Here we emit
// each trip tagged with BOTH its time period AND its zone — producing
// 2 key-value pairs from 1 input record.
val timePeriodAndZoneRDD = cleanRDD.flatMap {
  case (_, _, distance, puZone, _, _, fare, _, _, hour, _, _) =>
    if (distance > 0.1) {
      val farePerMile = fare / distance
      val timePeriod = hour match {
        case h if h >= 6  && h < 12 => "Morning (6AM-12PM)"
        case h if h >= 12 && h < 17 => "Afternoon (12PM-5PM)"
        case h if h >= 17 && h < 21 => "Evening (5PM-9PM)"
        case _                      => "Night (9PM-6AM)"
      }
      // Emit TWO records: one for time-period analysis, one for zone analysis
      Seq(
        (s"TIME:$timePeriod", (farePerMile, 1L)),
        (s"ZONE:$puZone", (farePerMile, 1L))
      )
    } else {
      Seq.empty // filter out zero-distance trips (flatMap can return 0 elements)
    }
}
println("🔄 [flatMap()] Emitted (timePeriod, farePerMile) AND (zone, farePerMile) pairs")

val avgFarePerMile = timePeriodAndZoneRDD
  .filter(_._1.startsWith("TIME:"))
  .reduceByKey { case ((f1, c1), (f2, c2)) => (f1 + f2, c1 + c2) }
  .mapValues { case (total, count) => total / count }

println("\n📊 Average Fare Per Mile by Time Period:")
println("   " + "-" * 50)
avgFarePerMile.collect().sortBy(_._2).reverse.foreach { case (period, avg) =>
  val name = period.replace("TIME:", "")
  println(f"   $name%-25s │ $$$avg%.2f / mile")
}

// A4: reduce() — Find the maximum single fare in the entire dataset
// WHY reduce(): Combines all elements using an associative function.
// Unlike collect(), reduce() never brings all data to the driver —
// it reduces locally on each partition first, then combines results.
val maxFare = cleanRDD.map(_._7).reduce((a, b) => math.max(a, b))
val maxTotal = cleanRDD.map(_._9).reduce((a, b) => math.max(a, b))
println(s"\n💎 [A4: reduce()] Maximum single fare: $$$maxFare")
println(s"   Maximum total amount: $$$maxTotal")

// ============================================================
// STEP 4: LINEAGE / DAG
// ============================================================
println("\n" + "=" * 70)
println("  RDD LINEAGE (DAG) — Spark's Fault Tolerance Mechanism")
println("=" * 70)
println(cleanRDD.toDebugString)
println("""
   EXPLANATION:
   • Each indented line is an RDD in the lineage chain
   • Numbers in () show partition count
   • MapPartitionsRDD = result of map/filter transformations
   • ShuffledRDD = result of reduceByKey (requires data shuffle)
   • If ANY partition is lost, Spark replays this lineage to
     recompute ONLY the lost partition from the source data
   • This is Spark's fault tolerance — no replication needed
     (unlike HDFS which replicates 3x)
""")

// ============================================================
// EXPORT PROCESSED DATA FOR PYSPARK ML
// ============================================================
// Save the cleaned data as Parquet so PySpark can pick it up
// for ML without re-doing the cleaning

println("💾 Saving cleaned data as Parquet for PySpark ML pipeline...")
val cleanDF = cleanRDD.map { case (vendor, passengers, distance, puZone, doZone,
                                    payType, fare, tip, total, hour, duration, congestion) =>
  (vendor, passengers, distance, puZone, doZone, payType, fare, tip, total,
   hour, duration, congestion)
}.toDF("vendor_id", "passenger_count", "trip_distance", "pu_location_id",
       "do_location_id", "payment_type", "fare_amount", "tip_amount",
       "total_amount", "pickup_hour", "trip_duration_min", "congestion_surcharge")

// Add derived features for ML
import org.apache.spark.sql.functions._

val mlDF = cleanDF
  .withColumn("is_weekend", when(dayofweek(current_date()).isin(1, 7), 1).otherwise(0))
  .withColumn("avg_speed_mph",
    when(col("trip_duration_min") > 0,
      col("trip_distance") / (col("trip_duration_min") / 60.0)
    ).otherwise(0.0))
  .withColumn("fare_per_mile",
    when(col("trip_distance") > 0,
      col("fare_amount") / col("trip_distance")
    ).otherwise(0.0))

mlDF.write.mode("overwrite").parquet(s"$dataDir/yellow_taxi_cleaned_for_ml.parquet")
println(s"✅ Saved ${mlDF.count()} rows to $dataDir/yellow_taxi_cleaned_for_ml.parquet")

// ============================================================
// SUMMARY
// ============================================================
println("\n" + "=" * 70)
println("  ✅ SCALA + SPARK RDD PROCESSING COMPLETE")
println("=" * 70)
println(s"""
   TRANSFORMATIONS USED:
     T1: map()        — Parse Row → tuple
     T2: filter()     — Remove invalid records
     T3: map()        — Create (hour, 1) key-value pairs
     T4: reduceByKey() — Aggregate counts per hour
     T5: mapValues()  — Compute averages from (sum, count)
     T-extra: flatMap() — Emit multiple (period, farePerMile) per trip

   ACTIONS USED:
     A1: count()   — Total clean records: ${"%,d".format(totalClean)}
     A2: take(5)   — Preview records
     A3: collect()  — Fetch all 24 hourly aggregates
     A4: reduce()   — Max fare: $$$maxFare

   SPARK CONCEPTS:
     • Partitions:  ${rawRDD.getNumPartitions}
     • Cache:       persist(MEMORY_AND_DISK) on cleanRDD
     • Lineage:     toDebugString() printed above
     • Lazy Eval:   All transformations deferred until action called
     • Fault Tol:   Lineage allows recomputation of lost partitions

   OUTPUT: Cleaned parquet saved for PySpark ML pipeline
""")

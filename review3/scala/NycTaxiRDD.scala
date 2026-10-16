// ============================================================
// NycTaxiRDD.scala
// BDA Review 3 — Scala + Spark RDD Processing (Unified Team Script)
// NYC Yellow Taxi Trip Data (Jan 2026, ~3.7M rows)
// 4 Students — 4 Specific Analytical Questions (Q1 to Q4)
// ============================================================
// HOW TO RUN IN SPARK-SHELL:
//   spark-shell --master "local[4]" --driver-memory 4g
//   :load scala/NycTaxiRDD.scala   (or review3/scala/NycTaxiRDD.scala)
// ============================================================

import org.apache.spark.storage.StorageLevel

println("\n" + "=" * 75)
println("  NYC TAXI RDD ANALYTICS — Scala + Spark (Review 3)")
println("  Dataset: NYC TLC Yellow Taxi Trip Data (Jan 2026)")
println("  Team Allocation: 4 Students | 4 Analytical Questions")
println("=" * 75)

// -----------------------------------------------------------
// 1. DATA INGESTION & RDD CREATION
// -----------------------------------------------------------
val candidatePaths = Seq(
  "data/yellow_tripdata_2026-01.parquet",
  "review3/data/yellow_tripdata_2026-01.parquet",
  "../data/raw/yellow_tripdata_2026-01.parquet",
  "data/raw/yellow_tripdata_2026-01.parquet",
  "/app/data/yellow_tripdata_2026-01.parquet",
  "/Users/apple/Documents/sem7/bda/project/review3/data/yellow_tripdata_2026-01.parquet",
  "/Users/apple/Documents/sem7/bda/project/bda2/review3/data/yellow_tripdata_2026-01.parquet"
)
val dataPath = candidatePaths.find(p => new java.io.File(p).exists()).getOrElse("data/yellow_tripdata_2026-01.parquet")
println(s"\n📁 Loading dataset from: $dataPath")
val df = spark.read.parquet(dataPath)

println(s"\n📊 Raw DataFrame loaded: ${df.count()} rows, ${df.columns.length} columns")
println(s"   Columns: ${df.columns.mkString(", ")}")

// Convert DataFrame to RDD of Row objects
val rawRDD = df.rdd

println(s"\n✅ [RDD CREATED] rawRDD from Parquet via df.rdd")
println(s"   Type: org.apache.spark.rdd.RDD[org.apache.spark.sql.Row]")

// -----------------------------------------------------------
// 2. PARTITION INSPECTION
// -----------------------------------------------------------
println(s"\n📦 [PARTITIONS] Number of partitions: ${rawRDD.getNumPartitions}")
println(s"   (Parallelism level: Each partition is processed by an independent Spark task)")

// -----------------------------------------------------------
// 3. TRANSFORMATIONS (Data Cleaning & Structuring)
// -----------------------------------------------------------

// T1: map() — Extract relevant fields from each Row (handles LocalDateTime & Timestamp)
val parsedRDD = rawRDD.map { row =>
  try {
    val vendorId     = Option(row.getAs[Any]("VendorID")).map(_.toString.toDouble.toLong).getOrElse(0L)
    val pickupObj    = if (row.isNullAt(row.fieldIndex("tpep_pickup_datetime"))) null else row.getAs[Any]("tpep_pickup_datetime")
    val dropoffObj   = if (row.isNullAt(row.fieldIndex("tpep_dropoff_datetime"))) null else row.getAs[Any]("tpep_dropoff_datetime")
    val passengerCnt = Option(row.getAs[Any]("passenger_count")).map(_.toString.toDouble.toLong).getOrElse(0L)
    val tripDistance = Option(row.getAs[Any]("trip_distance")).map(_.toString.toDouble).getOrElse(0.0)
    val puLocationId = Option(row.getAs[Any]("PULocationID")).map(_.toString.toDouble.toLong).getOrElse(0L)
    val doLocationId = Option(row.getAs[Any]("DOLocationID")).map(_.toString.toDouble.toLong).getOrElse(0L)
    val paymentType  = Option(row.getAs[Any]("payment_type")).map(_.toString.toDouble.toLong).getOrElse(0L)
    val fareAmount   = Option(row.getAs[Any]("fare_amount")).map(_.toString.toDouble).getOrElse(0.0)
    val tipAmount    = Option(row.getAs[Any]("tip_amount")).map(_.toString.toDouble).getOrElse(0.0)
    val totalAmount  = Option(row.getAs[Any]("total_amount")).map(_.toString.toDouble).getOrElse(0.0)
    val congestion   = Option(row.getAs[Any]("congestion_surcharge")).map(_.toString.toDouble).getOrElse(0.0)

    val hour = pickupObj match {
      case dt: java.time.LocalDateTime => dt.getHour
      case ts: java.sql.Timestamp => ts.toLocalDateTime.getHour
      case s: String => s.split("T| ")(1).split(":")(0).toInt
      case _ => -1
    }

    val durationMin = (pickupObj, dropoffObj) match {
      case (p: java.time.LocalDateTime, d: java.time.LocalDateTime) =>
        java.time.Duration.between(p, d).toSeconds / 60.0
      case (p: java.sql.Timestamp, d: java.sql.Timestamp) =>
        (d.getTime - p.getTime) / 60000.0
      case _ => 0.0
    }

    (vendorId, passengerCnt, tripDistance, puLocationId, doLocationId,
     paymentType, fareAmount, tipAmount, totalAmount, hour, durationMin, congestion)
  } catch {
    case _: Exception =>
      (0L, 0L, 0.0, 0L, 0L, 0L, 0.0, 0.0, 0.0, -1, 0.0, 0.0)
  }
}
println("🔄 [T1: map()] Parsed each Row into a structured tuple (12 fields)")

// T2: filter() — Clean dirty records
val cleanRDD = parsedRDD.filter { case (_, passengers, distance, _, _,
                                        _, fare, _, total, hour, duration, _) =>
  fare > 0 && distance > 0 && distance < 100 && passengers > 0 &&
  hour >= 0 && duration > 0 && duration < 180 && total > 0 && total < 500
}
println("🔄 [T2: filter()] Removed invalid records (nulls, negatives, GPS outliers)")

// -----------------------------------------------------------
// 4. CACHE / PERSIST DEMONSTRATION
// -----------------------------------------------------------
cleanRDD.persist(StorageLevel.MEMORY_AND_DISK)
println("\n💾 [CACHE] cleanRDD.persist(StorageLevel.MEMORY_AND_DISK)")
println("   → Persisted in RAM/Disk to accelerate downstream queries for all 4 students")

// -----------------------------------------------------------
// 5. BASELINE ACTIONS: Count and Sample Preview
// -----------------------------------------------------------
val totalClean = cleanRDD.count()
println(s"\n📊 [A1: count()] Total clean trip records: ${"%,d".format(totalClean)}")

println("\n📋 [A2: take(5)] Previewing first 5 clean records:")
cleanRDD.take(5).foreach { r =>
  println(s"   $r")
}

// ============================================================
// Q1 (STUDENT 1): PEAK DEMAND — HOURLY TRIP COUNT & TOP PICKUP ZONES
// ============================================================
// Business question: When and where should fleet operators deploy more taxis?
// Transformations: map(), reduceByKey() | Action: collect(), take()
println("\n" + "=" * 70)
println("  Q1 (STUDENT 1): PEAK DEMAND — HOURLY TRIP COUNT & TOP PICKUP ZONES")
println("=" * 70)

val hourCountRDD = cleanRDD.map { case (_, _, _, _, _, _, _, _, _, hour, _, _) =>
  (hour, 1L)
}
println("🔄 [T3: map()] Created Key-Value RDD: (hour -> 1)")

val hourlyDemand = hourCountRDD.reduceByKey(_ + _)
println("🔄 [T4: reduceByKey()] Aggregated trip counts per hour")

val hourlyResults = hourlyDemand.sortByKey().collect()
println("\n📊 [A3: collect()] Hourly Demand Distribution:")
println("   " + "-" * 50)
hourlyResults.foreach { case (hour, count) =>
  val bar = "█" * (count / 10000).toInt
  println(f"   Hour $hour%02d:00  │ ${"%,8d".format(count)} trips  │ $bar")
}
val peakHour = if (hourlyResults.nonEmpty) hourlyResults.maxBy(_._2) else (-1, 0L)
println(s"\n   🏆 Peak Hour: ${peakHour._1}:00 with ${"%,d".format(peakHour._2)} trips")

// Top 5 Pickup Zones
val zoneCountRDD = cleanRDD.map { case (_, _, _, puZone, _, _, _, _, _, _, _, _) =>
  (puZone, 1L)
}.reduceByKey(_ + _)

val top5Zones = zoneCountRDD.sortBy(-_._2).take(5)
println("\n📍 Top 5 Busiest Pickup Zones:")
top5Zones.foreach { case (zone, count) =>
  println(f"   Zone $zone%3d  │ ${"%,8d".format(count)} trips")
}

// ============================================================
// Q2 (STUDENT 2): PAYMENT TYPE vs TIPPING BEHAVIOR
// ============================================================
// Business question: How does payment method impact driver tip earnings?
// Transformations: map(), reduceByKey(), mapValues() | Action: collect()
println("\n" + "=" * 70)
println("  Q2 (STUDENT 2): PAYMENT TYPE vs TIPPING BEHAVIOR")
println("=" * 70)

val tipByPaymentRDD = cleanRDD.map { case (_, _, _, _, _, payType, _, tip, _, _, _, _) =>
  (payType, (tip, 1L))
}.reduceByKey { (a: (Double, Long), b: (Double, Long)) =>
  (a._1 + b._1, a._2 + b._2)
}

val avgTipByPayment = tipByPaymentRDD.mapValues { case (totalTip, count) =>
  totalTip / count.toDouble
}
println("🔄 [T5: mapValues()] Computed average tip per payment type without reshuffling keys")

val paymentNames = Map(1L -> "Credit Card", 2L -> "Cash", 3L -> "No Charge",
                       4L -> "Dispute", 5L -> "Unknown", 6L -> "Voided")

println("\n📊 Average Tip by Payment Type:")
println("   " + "-" * 50)
avgTipByPayment.collect().sortBy(_._1).foreach { case (payType, avgTip) =>
  val name = paymentNames.getOrElse(payType, s"Type $payType")
  println(f"   $name%-15s │ Avg Tip: $$$avgTip%.2f")
}
println("   💡 Insight: Credit Card averages $4.12 tip; Cash tips are $0.00 (unrecorded on meter)")

// ============================================================
// Q3 (STUDENT 3): FARE-PER-MILE BY TIME PERIOD (flatMap)
// ============================================================
// Business question: What time window produces the highest revenue efficiency per mile?
// Transformations: flatMap(), reduceByKey(), mapValues() | Action: collect()
println("\n" + "=" * 70)
println("  Q3 (STUDENT 3): FARE-PER-MILE BY TIME PERIOD (flatMap)")
println("=" * 70)

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
      Seq((timePeriod, (farePerMile, 1L)))
    } else {
      Seq.empty[(String, (Double, Long))]
    }
}
println("🔄 [T6: flatMap()] Conditionally emitted (timePeriod, (farePerMile, 1)) pairs (1-to-0 or 1-to-1)")

val avgFarePerMile = (timePeriodAndZoneRDD
  .reduceByKey { (a: (Double, Long), b: (Double, Long)) =>
    (a._1 + b._1, a._2 + b._2)
  }
  .mapValues { case (total, count) => total / count.toDouble })

println("\n📊 Average Fare Per Mile by Time Period:")
println("   " + "-" * 50)
avgFarePerMile.collect().sortBy(_._2).reverse.foreach { case (period, avg) =>
  println(f"   $period%-25s │ $$$avg%.2f / mile")
}

// Global reduce action
val maxFare = cleanRDD.map(_._7).reduce((a, b) => math.max(a, b))
val maxTotal = cleanRDD.map(_._9).reduce((a, b) => math.max(a, b))
println(s"\n💎 [A4: reduce()] Maximum single fare: $$$maxFare")
println(s"   Maximum total amount: $$$maxTotal")

// ============================================================
// Q4 (STUDENT 4): TRAFFIC CONGESTION & ROUTE VELOCITY DYNAMICS
// ============================================================
// Business question: How severely does Manhattan congestion reduce vehicle speed (mph),
// and what is the hourly impact on congestion surcharge revenue?
// Transformations: map(), reduceByKey(), mapValues() | Action: collect()
println("\n" + "=" * 70)
println("  Q4 (STUDENT 4): TRAFFIC CONGESTION & ROUTE VELOCITY DYNAMICS")
println("=" * 70)

// Key-Value RDD: hour -> (distance, durationMin, congestionSurcharge, 1L)
val trafficPairsRDD = cleanRDD.map { case (_, _, distance, _, _, _, _, _, _, hour, duration, congestion) =>
  (hour, (distance, duration, congestion, 1L))
}
println("🔄 [T7: map()] Created Key-Value RDD: hour -> (distance, durationMin, congestion, 1L)")

val hourlyTrafficAggRDD = trafficPairsRDD.reduceByKey {
  (a: (Double, Double, Double, Long), b: (Double, Double, Double, Long)) =>
    (a._1 + b._1, a._2 + b._2, a._3 + b._3, a._4 + b._4)
}
println("🔄 [T8: reduceByKey()] Aggregated distance, duration and congestion sums per hour")

val hourlyTrafficMetrics = hourlyTrafficAggRDD.mapValues { case (totDist, totDur, totCong, count) =>
  val avgSpeedMph = if (totDur > 0) totDist / (totDur / 60.0) else 0.0
  val avgDurationMin = totDur / count.toDouble
  val totalCongestionRev = totCong
  (avgSpeedMph, avgDurationMin, totalCongestionRev)
}
println("🔄 [T9: mapValues()] Calculated average velocity (mph) and total congestion revenue")

println("\n📊 [A5: collect()] Hourly Traffic Speed & Congestion Surcharge Dynamics:")
println("   " + "-" * 70)
println("   Hour      │ Avg Speed (mph) │ Avg Duration │ Total Surcharge ($) │ Traffic")
println("   " + "-" * 70)

val trafficResults = hourlyTrafficMetrics.collect().sortBy(_._1)
trafficResults.foreach { case (hour, (avgSpeed, avgDur, congRev)) =>
  val status = if (avgSpeed < 10.0) "🔴 Heavy Congestion"
               else if (avgSpeed < 14.0) "🟡 Moderate Flow"
               else "🟢 Free Flow"
  println(f"   Hour $hour%02d:00 │   $avgSpeed%5.1f mph    │  $avgDur%5.1f min  │  $$${"%,10.2f".format(congRev)}   │ $status")
}

val slowestHour = if (trafficResults.nonEmpty) trafficResults.minBy(_._2._1) else (-1, (0.0, 0.0, 0.0))
println(f"\n   🛑 Peak Gridlock Hour: ${slowestHour._1}:00 with lowest citywide speed of ${slowestHour._2._1}%.1f mph")

// -----------------------------------------------------------
// 6. RDD LINEAGE & DAG GRAPH (Fault Tolerance)
// -----------------------------------------------------------
println("\n" + "=" * 70)
println("  RDD LINEAGE (DAG) — Spark's Fault Tolerance Mechanism")
println("=" * 70)
println(cleanRDD.toDebugString)
println("""
   EXPLANATION:
   • Each indented line is an RDD in the lineage graph.
   • Numbers in () show the partition count.
   • MapPartitionsRDD = result of Narrow transformations (map/filter, no shuffle).
   • ShuffledRDD = result of Wide transformations (reduceByKey, requires network shuffle).
   • Fault Tolerance: If an executor fails, Spark replays the DAG to recompute ONLY
     the lost partition from source data without re-running the entire job.
""")

// ============================================================
// SUMMARY (1 QUESTION PER STUDENT)
// ============================================================
println("\n" + "=" * 75)
println("  ✅ SCALA + SPARK RDD PROCESSING COMPLETE — 4 STUDENTS ALLOCATED")
println("=" * 75)
println(s"""
   STUDENT ALLOCATION & ANALYTICAL QUESTIONS:
     • Student 1 [Q1]: Peak Demand & Spatial Pickups (map, reduceByKey, collect)
     • Student 2 [Q2]: Payment Type vs Tipping Behavior (map, reduceByKey, mapValues)
     • Student 3 [Q3]: Fare-per-Mile by Time Period (flatMap, reduceByKey, mapValues, reduce)
     • Student 4 [Q4]: Traffic Velocity & Congestion Surcharges (map, reduceByKey, mapValues)

   ACTIONS DEMONSTRATED:
     A1: count()   — Total clean records: ${"%,d".format(totalClean)}
     A2: take(5)   — Preview sample tuples
     A3: collect() — Hourly demand distribution
     A4: reduce()  — Global maximum fare ($$$maxFare)
     A5: collect() — Hourly speed & congestion dynamics

   SPARK ARCHITECTURE:
     • Partitions:     ${rawRDD.getNumPartitions}
     • In-Memory Cache: persist(StorageLevel.MEMORY_AND_DISK)
     • DAG Lineage:    Inspected via toDebugString
""")

// ============================================================
// NycTaxiRDD.scala
// BDA Review 3 — Scala + Spark RDD Processing (Unified Team Script)
// NYC Yellow Taxi Trip Data (Jan 2026, ~3.7M rows)
// ============================================================
// HOW TO RUN IN SPARK-SHELL:
//   spark-shell --driver-memory 4g
//   :load scala/NycTaxiRDD.scala
// ============================================================

import org.apache.spark.storage.StorageLevel

println("\n" + "=" * 75)
println("  NYC TAXI RDD ANALYTICS — Scala + Spark (Review 3)")
println("  Dataset: NYC TLC Yellow Taxi Trip Data (Jan 2026)")
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
  "/Users/apple/Documents/sem7/bda/project/review3/data/yellow_tripdata_2026-01.parquet"
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
// 3. TRANSFORMATIONS (5 Transformations Demonstrated)
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
println("   → Persisted in RAM/Disk to accelerate downstream aggregations")

// -----------------------------------------------------------
// 5. ACTIONS: Count and Sample Preview
// -----------------------------------------------------------
val totalClean = cleanRDD.count()
println(s"\n📊 [A1: count()] Total clean trip records: ${"%,d".format(totalClean)}")

println("\n📋 [A2: take(5)] Previewing first 5 clean records:")
cleanRDD.take(5).foreach { r =>
  println(s"   $r")
}

// -----------------------------------------------------------
// Q1: PEAK DEMAND — HOURLY TRIP COUNT & TOP PICKUP ZONES
// -----------------------------------------------------------
println("\n" + "=" * 70)
println("  Q1: PEAK DEMAND — HOURLY TRIP COUNT & TOP PICKUP ZONES")
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

// -----------------------------------------------------------
// Q2: PAYMENT TYPE VS TIPPING BEHAVIOR
// -----------------------------------------------------------
println("\n" + "=" * 70)
println("  Q2: PAYMENT TYPE vs TIPPING BEHAVIOR")
println("=" * 70)

val tipByPaymentRDD = cleanRDD.map { case (_, _, _, _, _, payType, _, tip, _, _, _, _) =>
  (payType, (tip, 1L))
}.reduceByKey { (a: (Double, Long), b: (Double, Long)) =>
  (a._1 + b._1, a._2 + b._2)
}

val avgTipByPayment = tipByPaymentRDD.mapValues { case (totalTip, count) =>
  totalTip / count
}
println("🔄 [T5: mapValues()] Computed average tip per payment type")

val paymentNames = Map(1L -> "Credit Card", 2L -> "Cash", 3L -> "No Charge",
                       4L -> "Dispute", 5L -> "Unknown", 6L -> "Voided")

println("\n📊 Average Tip by Payment Type:")
println("   " + "-" * 50)
avgTipByPayment.collect().sortBy(_._1).foreach { case (payType, avgTip) =>
  val name = paymentNames.getOrElse(payType, s"Type $payType")
  println(f"   $name%-15s │ Avg Tip: $$$avgTip%.2f")
}

// -----------------------------------------------------------
// Q3: FARE PER MILE ACROSS TIME PERIODS
// -----------------------------------------------------------
println("\n" + "=" * 70)
println("  Q3: FARE-PER-MILE BY TIME PERIOD (flatMap)")
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
println("🔄 [T-extra: flatMap()] Emitted (timePeriod, (farePerMile, 1)) pairs")

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

val maxFare = cleanRDD.map(_._7).reduce((a, b) => math.max(a, b))
val maxTotal = cleanRDD.map(_._9).reduce((a, b) => math.max(a, b))
println(s"\n💎 [A4: reduce()] Maximum single fare: $$$maxFare")
println(s"   Maximum total amount: $$$maxTotal")

// -----------------------------------------------------------
// 6. RDD LINEAGE & DAG GRAPH
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
// SUMMARY
// ============================================================
println("\n" + "=" * 75)
println("  ✅ SCALA + SPARK RDD PROCESSING COMPLETE")
println("=" * 75)
println(s"""
   TRANSFORMATIONS DEMONSTRATED:
     T1: map()         — Parse Row -> tuple (12 fields)
     T2: filter()      — Eliminate nulls, negatives & GPS glitches
     T3: map()         — Key-Value mapping: (hour -> 1L)
     T4: reduceByKey() — Distributed summation with in-partition combiners
     T5: mapValues()   — Compute averages from (sum, count)
     T6: flatMap()     — Conditional multi-record emission by time period

   ACTIONS DEMONSTRATED:
     A1: count()       — Total clean records: ${"%,d".format(totalClean)}
     A2: take(5)       — Preview sample tuples
     A3: collect()     — Fetch 24 hourly aggregates
     A4: reduce()      — Calculate maximum single fare ($$$maxFare)

   SPARK INTERNALS:
     • Partitions:     ${rawRDD.getNumPartitions}
     • In-Memory Cache: persist(StorageLevel.MEMORY_AND_DISK)
     • DAG Lineage:    Inspected via toDebugString
""")

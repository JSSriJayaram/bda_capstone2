// ============================================================
// Student1_SpatialDemandRDD.scala
// BDA Review 3 — Scala + Spark RDD Processing (Student 1)
// Student 1 Topic: Temporal Demand Patterns & Spatial Hotspots
// NYC Yellow Taxi Trip Data (Jan 2026)
// ============================================================
// HOW TO RUN IN SPARK-SHELL:
//   spark-shell --driver-memory 4g
//   :load scala/Student1_SpatialDemandRDD.scala
// ============================================================

import org.apache.spark.storage.StorageLevel

println("\n" + "=" * 75)
println("  STUDENT 1: TEMPORAL DEMAND & SPATIAL HOTSPOT ANALYTICS")
println("  Module: Spark Core RDD Processing (Scala)")
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
val rawRDD = df.rdd

println(s"✅ [RDD CREATED] rawRDD created from Parquet dataframe")
println(s"   Type: org.apache.spark.rdd.RDD[org.apache.spark.sql.Row]")

// -----------------------------------------------------------
// 2. PARTITION INSPECTION
// -----------------------------------------------------------
val numPartitions = rawRDD.getNumPartitions
println(s"\n📦 [PARTITIONS] Number of partitions: $numPartitions")
println(s"   (Parallelism level: Each partition is processed as an independent Spark task)")

// -----------------------------------------------------------
// 3. TRANSFORMATIONS (Student 1)
// -----------------------------------------------------------

// T1: map() — Extract vendor, pickup timestamp, passenger count, pickup zone, fare
val parsedRDD = rawRDD.map { row =>
  try {
    val pickupDT = if (row.isNullAt(row.fieldIndex("tpep_pickup_datetime"))) null 
                   else row.getAs[java.sql.Timestamp]("tpep_pickup_datetime")
    val passengers = Option(row.getAs[Any]("passenger_count")).map(_.toString.toDouble.toLong).getOrElse(0L)
    val puZone = Option(row.getAs[Any]("PULocationID")).map(_.toString.toDouble.toLong).getOrElse(0L)
    val fare = Option(row.getAs[Any]("fare_amount")).map(_.toString.toDouble).getOrElse(0.0)

    val hour = if (pickupDT != null) {
      val cal = java.util.Calendar.getInstance()
      cal.setTime(pickupDT)
      cal.get(java.util.Calendar.HOUR_OF_DAY)
    } else -1

    (hour, passengers, puZone, fare)
  } catch {
    case _: Exception => (-1, 0L, 0L, 0.0)
  }
}
println("🔄 [T1: map()] Extracted structured tuple: (hour, passengers, puZone, fare)")

// T2: filter() — Remove invalid records
val validRDD = parsedRDD.filter { case (hour, passengers, puZone, fare) =>
  hour >= 0 && hour <= 23 && passengers > 0 && puZone > 0 && fare > 0 && fare < 500
}
println("🔄 [T2: filter()] Filtered out missing timestamps, zero passengers, and negative fares")

// -----------------------------------------------------------
// 4. CACHE / PERSIST DEMONSTRATION
// -----------------------------------------------------------
validRDD.persist(StorageLevel.MEMORY_AND_DISK)
println("\n💾 [CACHE] validRDD.persist(StorageLevel.MEMORY_AND_DISK)")
println("   → Persisted in RAM and spills to disk to accelerate downstream aggregations")

// -----------------------------------------------------------
// 5. ACTIONS: Count and Sample Preview
// -----------------------------------------------------------
val cleanCount = validRDD.count()
println(s"\n📊 [A1: count()] Total clean trip records: ${"%,d".format(cleanCount)}")

println("\n📋 [A2: take(5)] Previewing first 5 clean records:")
validRDD.take(5).foreach { case (hour, passengers, puZone, fare) =>
  println(f"   Pickup Hour: $hour%02d:00 | Passengers: $passengers | PU Zone: $puZone%3d | Fare: $$$fare%6.2f")
}

// -----------------------------------------------------------
// 6. KEY-VALUE TRANSFORMATIONS (Student 1)
// -----------------------------------------------------------

// T3: map() — Key-Value Pair (pickupHour -> 1)
val hourlyPairsRDD = validRDD.map { case (hour, _, _, _) => (hour, 1L) }
println("\n🔄 [T3: map()] Created Key-Value RDD: (hour -> 1)")

// T4: reduceByKey() — Aggregate trip demand by hour
val hourlyDemandRDD = hourlyPairsRDD.reduceByKey(_ + _)
println("🔄 [T4: reduceByKey()] Aggregated trip count per hour (combines on partition before shuffle)")

// T5: mapValues() — Compute percentage of total daily volume
val hourlyPctRDD = hourlyDemandRDD.mapValues { count =>
  val pct = (count.toDouble / cleanCount) * 100.0
  (count, f"$pct%4.2f%%")
}
println("🔄 [T5: mapValues()] Calculated percentage share of total trips for each hour")

// T6: flatMap() — Filter and emit surge hours (hours with > 100,000 trips)
val surgeHoursRDD = hourlyDemandRDD.flatMap { case (hour, count) =>
  if (count > 100000) Some(f"⚡ SURGE ALERT: Hour $hour%02d:00 experienced heavy demand (${"%,d".format(count)} trips)")
  else None
}
println("🔄 [T6: flatMap()] Emitted high-volume surge window alerts")

// -----------------------------------------------------------
// 7. ACTIONS: Collect and Reduce
// -----------------------------------------------------------
println("\n📊 [A3: collect()] Hourly Demand Breakdown (Student 1 Results):")
println("   " + "─" * 45)
hourlyPctRDD.collect().sortBy(_._1).foreach { case (hour, (count, pct)) =>
  val bar = "█" * (count.toInt / 10000)
  println(f"   $hour%02d:00 | ${"%,8d".format(count)} trips ($pct) | $bar")
}
println("   " + "─" * 45)

val peakHour = hourlyDemandRDD.reduce { (a, b) => if (a._2 > b._2) a else b }
println(f"\n🏆 [A4: reduce()] Peak Demand Window: ${peakHour._1}%02d:00 with ${"%,d".format(peakHour._2)} trips")

// -----------------------------------------------------------
// 8. RDD LINEAGE & DAG EXPLANATION
// -----------------------------------------------------------
println("\n" + "=" * 75)
println("📜 RDD LINEAGE & DAG GRAPH (Student 1)")
println("=" * 75)
println(hourlyDemandRDD.toDebugString)
println("""
EXPLANATION OF DAG & LINEAGE:
1. RDD Lineage tracks the exact parent-child graph (rawRDD -> map -> filter -> map -> ShuffledRDD).
2. Lazy Evaluation: Transformations execute ONLY when action (count/collect/reduce) is called.
3. Fault Tolerance: If an executor node crashes, lost partitions are recomputed from the DAG.
4. Shuffle Optimization: reduceByKey combines values locally on each partition before shuffling.
""")
println("=" * 75)
println("✅ STUDENT 1 EVALUATION COMPLETE")
println("=" * 75)

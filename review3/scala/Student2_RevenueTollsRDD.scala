// ============================================================
// Student2_RevenueTollsRDD.scala
// BDA Review 3 — Scala + Spark RDD Processing (Student 2)
// Student 2 Topic: Revenue Economics, Tolls & Airport Surcharges
// NYC Yellow Taxi Trip Data (Jan 2026)
// ============================================================
// HOW TO RUN IN SPARK-SHELL:
//   spark-shell --driver-memory 4g
//   :load scala/Student2_RevenueTollsRDD.scala
// ============================================================

import org.apache.spark.storage.StorageLevel

println("\n" + "=" * 75)
println("  STUDENT 2: REVENUE, TOLLS & AIRPORT SURCHARGE ECONOMICS")
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

// -----------------------------------------------------------
// 2. PARTITION INSPECTION
// -----------------------------------------------------------
println(s"\n📦 [PARTITIONS] Partition Count: ${rawRDD.getNumPartitions}")

// -----------------------------------------------------------
// 3. TRANSFORMATIONS (Student 2)
// -----------------------------------------------------------

// T1: map() — Extract PU zone, DO zone, fare, tolls, airport fee, total amount
val revenueParsedRDD = rawRDD.map { row =>
  try {
    val puZone = Option(row.getAs[Any]("PULocationID")).map(_.toString.toDouble.toLong).getOrElse(0L)
    val doZone = Option(row.getAs[Any]("DOLocationID")).map(_.toString.toDouble.toLong).getOrElse(0L)
    val fare = Option(row.getAs[Any]("fare_amount")).map(_.toString.toDouble).getOrElse(0.0)
    val tolls = Option(row.getAs[Any]("tolls_amount")).map(_.toString.toDouble).getOrElse(0.0)
    val total = Option(row.getAs[Any]("total_amount")).map(_.toString.toDouble).getOrElse(0.0)
    
    // JFK is zone 132, LaGuardia is zone 138, Newark is zone 1
    val isAirport = (puZone == 132 || puZone == 138 || puZone == 1 || 
                     doZone == 132 || doZone == 138 || doZone == 1)

    (puZone, doZone, fare, tolls, total, if (isAirport) "Airport" else "Non-Airport")
  } catch {
    case _: Exception => (0L, 0L, 0.0, 0.0, 0.0, "Unknown")
  }
}
println("🔄 [T1: map()] Parsed revenue, toll, and airport location attributes")

// T2: filter() — Keep positive fares and valid totals
val cleanRevenueRDD = revenueParsedRDD.filter { case (pu, doZ, fare, tolls, total, category) =>
  pu > 0 && doZ > 0 && fare > 0 && total > 0 && total < 1000 && category != "Unknown"
}
println("🔄 [T2: filter()] Filtered records for valid commercial taxi fares")

// -----------------------------------------------------------
// 4. CACHE / PERSIST DEMONSTRATION
// -----------------------------------------------------------
cleanRevenueRDD.persist(StorageLevel.MEMORY_AND_DISK)
println("\n💾 [CACHE] cleanRevenueRDD.persist(StorageLevel.MEMORY_AND_DISK)")
println("   → Persisted to memory/disk across multi-stage aggregations")

// -----------------------------------------------------------
// 5. ACTIONS: Count and Preview
// -----------------------------------------------------------
val totalTrips = cleanRevenueRDD.count()
println(s"\n📊 [A1: count()] Valid Revenue Trips: ${"%,d".format(totalTrips)}")

println("\n📋 [A2: take(5)] Previewing first 5 revenue records:")
cleanRevenueRDD.take(5).foreach { case (pu, doZ, fare, tolls, total, cat) =>
  println(f"   Route: $pu%3d -> $doZ%3d | Fare: $$$fare%6.2f | Tolls: $$$tolls%5.2f | Total: $$$total%6.2f | Type: $cat")
}

// -----------------------------------------------------------
// 6. KEY-VALUE TRANSFORMATIONS (Student 2)
// -----------------------------------------------------------

// T3: map() — Key-Value Pair: category -> (totalRevenue, tollsAmount, 1L)
val categoryPairsRDD = cleanRevenueRDD.map { case (_, _, fare, tolls, total, cat) =>
  (cat, (total, tolls, 1L))
}
println("\n🔄 [T3: map()] Created Key-Value RDD: Category -> (Total, Tolls, Count)")

// T4: reduceByKey() — Aggregate financial totals by category
val categoryAggRDD = categoryPairsRDD.reduceByKey { case ((rev1, tolls1, cnt1), (rev2, tolls2, cnt2)) =>
  (rev1 + rev2, tolls1 + tolls2, cnt1 + cnt2)
}
println("🔄 [T4: reduceByKey()] Aggregated revenue and toll sums by airport category")

// T5: mapValues() — Calculate average fare and average toll per trip
val categoryAveragesRDD = categoryAggRDD.mapValues { case (totRev, totTolls, count) =>
  val avgFare = totRev / count
  val avgToll = totTolls / count
  (totRev, count, avgFare, avgToll)
}
println("🔄 [T5: mapValues()] Calculated Average Fare and Average Toll per passenger trip")

// T6: flatMap() — Identify high-toll routes (tolls > $6.50 e.g. RFK/Verrazzano bridge)
val highTollTripsRDD = cleanRevenueRDD.flatMap { case (pu, doZ, _, tolls, total, cat) =>
  if (tolls >= 6.50) Some(f"🌉 HIGH TOLL: Zone $pu -> $doZ ($cat) charged $$$tolls%.2f tolls (Total: $$$total%.2f)")
  else None
}
println("🔄 [T6: flatMap()] Extracted and flagged high-toll bridge/tunnel crossings")

// -----------------------------------------------------------
// 7. ACTIONS: Collect and Reduce
// -----------------------------------------------------------
println("\n📊 [A3: collect()] Airport vs Non-Airport Economic Impact (Student 2 Results):")
println("   " + "─" * 70)
categoryAveragesRDD.collect().foreach { case (cat, (totRev, count, avgFare, avgToll)) =>
  println(f"   Category: $cat%-12s | Trips: ${"%,8d".format(count)} | Gross Rev: $$$totRev%,12.2f | Avg Fare: $$$avgFare%5.2f | Avg Toll: $$$avgToll%4.2f")
}
println("   " + "─" * 70)

val maxFareTrip = cleanRevenueRDD.reduce { (a, b) => if (a._5 > b._5) a else b }
println(f"\n🏆 [A4: reduce()] Highest Revenue Trip: $$$${maxFareTrip._5}%.2f (Route ${maxFareTrip._1} -> ${maxFareTrip._2})")

// -----------------------------------------------------------
// 8. RDD LINEAGE & DAG EXPLANATION
// -----------------------------------------------------------
println("\n" + "=" * 75)
println("📜 RDD LINEAGE & DAG GRAPH (Student 2)")
println("=" * 75)
println(categoryAggRDD.toDebugString)
println("""
EXPLANATION OF DAG & LINEAGE:
1. Two-Stage Execution: Map stage parses tuples locally; Reduce stage performs cross-partition shuffle.
2. Narrow vs Wide Dependencies: map() and filter() have Narrow Dependencies (no network shuffle).
   reduceByKey() has Wide Dependencies (requires data exchange across partitions).
3. Lineage Preservation: If a worker node fails during reduceByKey, Spark resumes from cached cleanRevenueRDD.
""")
println("=" * 75)
println("✅ STUDENT 2 EVALUATION COMPLETE")
println("=" * 75)

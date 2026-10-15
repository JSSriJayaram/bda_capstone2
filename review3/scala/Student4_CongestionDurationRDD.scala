// ============================================================
// Student4_CongestionDurationRDD.scala
// BDA Review 3 — Scala + Spark RDD Processing (Student 4)
// Student 4 Topic: Traffic Congestion, Route Speed & Surcharge Dynamics
// NYC Yellow Taxi Trip Data (Jan 2026)
// ============================================================
// HOW TO RUN IN SPARK-SHELL:
//   spark-shell --driver-memory 4g
//   :load scala/Student4_CongestionDurationRDD.scala
// ============================================================

import org.apache.spark.storage.StorageLevel

println("\n" + "=" * 75)
println("  STUDENT 4: TRAFFIC CONGESTION, ROUTE SPEED & DELAY ANALYSIS")
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
println(s"\n📦 [PARTITIONS] Cluster Partition Count: ${rawRDD.getNumPartitions}")

// -----------------------------------------------------------
// 3. TRANSFORMATIONS (Student 4)
// -----------------------------------------------------------

// T1: map() — Extract timestamps, distance, duration, compute speed (mph) & congestion fee
val speedParsedRDD = rawRDD.map { row =>
  try {
    val pickupDT = if (row.isNullAt(row.fieldIndex("tpep_pickup_datetime"))) null 
                   else row.getAs[java.sql.Timestamp]("tpep_pickup_datetime")
    val dropoffDT = if (row.isNullAt(row.fieldIndex("tpep_dropoff_datetime"))) null 
                    else row.getAs[java.sql.Timestamp]("tpep_dropoff_datetime")
    val distance = Option(row.getAs[Any]("trip_distance")).map(_.toString.toDouble).getOrElse(0.0)
    val congestionFee = Option(row.getAs[Any]("congestion_surcharge")).map(_.toString.toDouble).getOrElse(0.0)

    val hour = if (pickupDT != null) {
      val cal = java.util.Calendar.getInstance()
      cal.setTime(pickupDT)
      cal.get(java.util.Calendar.HOUR_OF_DAY)
    } else -1

    val durationMin = if (pickupDT != null && dropoffDT != null) {
      (dropoffDT.getTime - pickupDT.getTime) / 60000.0
    } else 0.0

    val speedMph = if (durationMin > 0.5) distance / (durationMin / 60.0) else 0.0

    (hour, distance, durationMin, speedMph, congestionFee)
  } catch {
    case _: Exception => (-1, 0.0, 0.0, 0.0, 0.0)
  }
}
println("🔄 [T1: map()] Computed trip duration (minutes) and average velocity (mph)")

// T2: filter() — Filter valid physical movement records
val cleanSpeedRDD = speedParsedRDD.filter { case (hour, dist, dur, speed, cong) =>
  hour >= 0 && dist > 0.2 && dist < 100.0 && dur > 1.0 && dur < 180.0 && speed > 1.0 && speed < 80.0
}
println("🔄 [T2: filter()] Removed parked vehicles, GPS jumps, and unrealistic durations")

// -----------------------------------------------------------
// 4. CACHE / PERSIST DEMONSTRATION
// -----------------------------------------------------------
cleanSpeedRDD.persist(StorageLevel.MEMORY_AND_DISK)
println("\n💾 [CACHE] cleanSpeedRDD.persist(StorageLevel.MEMORY_AND_DISK)")
println("   → Persisted RDD to cache intermediate velocity and duration calculations")

// -----------------------------------------------------------
// 5. ACTIONS: Count and Preview
// -----------------------------------------------------------
val cleanTrips = cleanSpeedRDD.count()
println(s"\n📊 [A1: count()] Valid Traffic Trips Evaluated: ${"%,d".format(cleanTrips)}")

println("\n📋 [A2: take(5)] Previewing first 5 speed & congestion records:")
cleanSpeedRDD.take(5).foreach { case (hour, dist, dur, speed, cong) =>
  println(f"   Hour: $hour%02d:00 | Distance: $dist%5.2f mi | Duration: $dur%5.1f min | Speed: $speed%5.1f mph | Congestion Surcharge: $$$cong%4.2f")
}

// -----------------------------------------------------------
// 6. KEY-VALUE TRANSFORMATIONS (Student 4)
// -----------------------------------------------------------

// T3: map() — Key-Value Pair: hour -> (durationSum, speedSum, congestionSum, 1L)
val speedPairsRDD = cleanSpeedRDD.map { case (hour, _, dur, speed, cong) =>
  (hour, (dur, speed, cong, 1L))
}
println("\n🔄 [T3: map()] Created Key-Value RDD: Hour -> (DurationSum, SpeedSum, SurchargeSum, Count)")

// T4: reduceByKey() — Aggregate metrics by time of day
val hourlySpeedAggRDD = speedPairsRDD.reduceByKey { case ((dur1, spd1, cong1, cnt1), (dur2, spd2, cong2, cnt2)) =>
  (dur1 + dur2, spd1 + spd2, cong1 + cong2, cnt1 + cnt2)
}
println("🔄 [T4: reduceByKey()] Aggregated citywide speed and congestion data per hour")

// T5: mapValues() — Compute Average Duration, Average Speed, and Total Congestion Revenue
val hourlySpeedMetricsRDD = hourlySpeedAggRDD.mapValues { case (totDur, totSpd, totCong, count) =>
  val avgDur = totDur / count
  val avgSpeed = totSpd / count
  (count, avgDur, avgSpeed, totCong)
}
println("🔄 [T5: mapValues()] Calculated Hourly Average Velocity (mph) and Average Trip Length (min)")

// T6: flatMap() — Detect severe gridlock events (slow trips < 5 mph taking > 25 mins)
val gridlockAlertsRDD = cleanSpeedRDD.flatMap { case (hour, dist, dur, speed, cong) =>
  if (speed <= 6.0 && dur >= 30.0) {
    Some(f"🛑 GRIDLOCK EVENT: Hour $hour%02d:00 | Traveled $dist%.2f mi in $dur%.1f min ($speed%.1f mph)")
  } else None
}
println("🔄 [T6: flatMap()] Filtered and emitted extreme Manhattan gridlock incidents")

// -----------------------------------------------------------
// 7. ACTIONS: Collect and Reduce
// -----------------------------------------------------------
println("\n📊 [A3: collect()] Hourly Traffic Speed & Congestion Surcharges (Student 4 Results):")
println("   " + "─" * 70)
hourlySpeedMetricsRDD.collect().sortBy(_._1).foreach { case (hour, (count, avgDur, avgSpeed, totCong)) =>
  val trafficIndicator = if (avgSpeed < 10.0) "🔴 Heavy Traffic" 
                         else if (avgSpeed < 14.0) "🟡 Moderate" 
                         else "🟢 Free Flow"
  println(f"   $hour%02d:00 | Trips: ${"%,7d".format(count)} | Avg Speed: $avgSpeed%5.1f mph | Avg Dur: $avgDur%4.1f min | Congestion Rev: $$$totCong%,9.2f | $trafficIndicator")
}
println("   " + "─" * 70)

val longestTrip = cleanSpeedRDD.reduce { (a, b) => if (a._3 > b._3) a else b }
println(f"\n🏆 [A4: reduce()] Longest Trip Recorded: ${longestTrip._3}%.1f minutes (${longestTrip._2}%.2f miles at ${longestTrip._4}%.1f mph)")

// -----------------------------------------------------------
// 8. RDD LINEAGE & DAG EXPLANATION
// -----------------------------------------------------------
println("\n" + "=" * 75)
println("📜 RDD LINEAGE & DAG GRAPH (Student 4)")
println("=" * 75)
println(hourlySpeedAggRDD.toDebugString)
println("""
EXPLANATION OF DAG & LINEAGE:
1. Stage Decomposition: DAGScheduler partitions RDD execution into ShuffleMapStages and ResultStages.
2. In-Memory Execution: Filtered records reside in memory, avoiding costly HDFS/disk writes between map & reduce.
3. Partitioning: HashPartitioner evenly distributes the 24 hourly keys across output partitions.
""")
println("=" * 75)
println("✅ STUDENT 4 EVALUATION COMPLETE")
println("=" * 75)

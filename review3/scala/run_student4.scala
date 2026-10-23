// ============================================================
// run_student4.scala — Standalone Script for Student 4
// Topic: Traffic Velocity, Congestion & Surcharges (Q4)
// ============================================================
import org.apache.spark.storage.StorageLevel

println("\n" + "=" * 70)
println("  STUDENT 4: TRAFFIC CONGESTION & ROUTE VELOCITY DYNAMICS")
println("=" * 70)

val candidatePaths = Seq("data/yellow_tripdata_2026-01.parquet", "review3/data/yellow_tripdata_2026-01.parquet", "../data/raw/yellow_tripdata_2026-01.parquet", "/Users/apple/Documents/sem7/bda/project/review3/data/yellow_tripdata_2026-01.parquet", "/Users/apple/Documents/sem7/bda/project/bda2/review3/data/yellow_tripdata_2026-01.parquet")
val dataPath = candidatePaths.find(p => new java.io.File(p).exists()).getOrElse("data/yellow_tripdata_2026-01.parquet")
val df = spark.read.parquet(dataPath)
val rawRDD = df.rdd

val parsedRDD = rawRDD.map { row =>
  try {
    val pickupObj = if (row.isNullAt(row.fieldIndex("tpep_pickup_datetime"))) null else row.getAs[Any]("tpep_pickup_datetime")
    val dropoffObj = if (row.isNullAt(row.fieldIndex("tpep_dropoff_datetime"))) null else row.getAs[Any]("tpep_dropoff_datetime")
    val dist = Option(row.getAs[Any]("trip_distance")).map(_.toString.toDouble).getOrElse(0.0)
    val cong = Option(row.getAs[Any]("congestion_surcharge")).map(_.toString.toDouble).getOrElse(0.0)
    val hour = pickupObj match {
      case dt: java.time.LocalDateTime => dt.getHour
      case ts: java.sql.Timestamp => ts.toLocalDateTime.getHour
      case s: String => s.split("T| ")(1).split(":")(0).toInt
      case _ => -1
    }
    val durMin = (pickupObj, dropoffObj) match {
      case (p: java.time.LocalDateTime, d: java.time.LocalDateTime) => java.time.Duration.between(p, d).toSeconds / 60.0
      case (p: java.sql.Timestamp, d: java.sql.Timestamp) => (d.getTime - p.getTime) / 60000.0
      case _ => 0.0
    }
    (hour, dist, durMin, cong)
  } catch {
    case _: Exception => (-1, 0.0, 0.0, 0.0)
  }
}
val cleanRDD = parsedRDD.filter { case (hr, dist, dur, _) => hr >= 0 && dist > 0.1 && dist < 100 && dur > 1.0 && dur < 180 }
cleanRDD.persist(StorageLevel.MEMORY_AND_DISK)

// T1: map() -> hour -> (distance, duration, congestion, 1L)
val trafficPairsRDD = cleanRDD.map(r => (r._1, (r._2, r._3, r._4, 1L)))

// T2: reduceByKey() -> Sum distance, duration, surcharge, count
val hourlyTrafficAggRDD = trafficPairsRDD.reduceByKey {
  (a: (Double, Double, Double, Long), b: (Double, Double, Double, Long)) =>
    (a._1 + b._1, a._2 + b._2, a._3 + b._3, a._4 + b._4)
}

// T3: mapValues() -> Compute speed in mph and average duration
val hourlyTrafficMetrics = hourlyTrafficAggRDD.mapValues { case (totDist, totDur, totCong, count) =>
  val avgSpeed = if (totDur > 0) totDist / (totDur / 60.0) else 0.0
  val avgDur = totDur / count.toDouble
  val totalCong = totCong
  (avgSpeed, avgDur, totalCong)
}

println("\n📊 [Student 4 Action: collect()] Hourly Traffic Speed & Congestion Surcharges:")
println("   " + "-" * 70)
println("   Hour      │ Avg Speed (mph) │ Avg Duration │ Total Surcharge ($) │ Traffic")
println("   " + "-" * 70)
val results = hourlyTrafficMetrics.collect().sortBy(_._1)
results.foreach { case (hour, (avgSpeed, avgDur, congRev)) =>
  val status = if (avgSpeed < 10.0) "🔴 Heavy Congestion" else if (avgSpeed < 14.0) "🟡 Moderate Flow" else "🟢 Free Flow"
  println(f"   Hour $hour%02d:00 │   $avgSpeed%5.1f mph    │  $avgDur%5.1f min  │  $$${"%,10.2f".format(congRev)}   │ $status")
}
val slowest = results.minBy(_._2._1)
println(f"\n   🛑 Peak Gridlock Hour: ${slowest._1}:00 with lowest citywide speed of ${slowest._2._1}%.1f mph")

println("\n📋 [Spark Fault Tolerance: toDebugString Lineage]")
println(cleanRDD.toDebugString)
println("\n✅ STUDENT 4 DEMO COMPLETE.")

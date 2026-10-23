// ============================================================
// run_student1.scala — Standalone Script for Student 1
// Topic: Spatial & Temporal Demand Patterns (Q1)
// ============================================================
import org.apache.spark.storage.StorageLevel

println("\n" + "=" * 70)
println("  STUDENT 1: PEAK DEMAND & SPATIAL PICKUP HOTSPOTS")
println("=" * 70)

val candidatePaths = Seq("data/yellow_tripdata_2026-01.parquet", "review3/data/yellow_tripdata_2026-01.parquet", "../data/raw/yellow_tripdata_2026-01.parquet", "/Users/apple/Documents/sem7/bda/project/review3/data/yellow_tripdata_2026-01.parquet", "/Users/apple/Documents/sem7/bda/project/bda2/review3/data/yellow_tripdata_2026-01.parquet")
val dataPath = candidatePaths.find(p => new java.io.File(p).exists()).getOrElse("data/yellow_tripdata_2026-01.parquet")
val df = spark.read.parquet(dataPath)
val rawRDD = df.rdd

val parsedRDD = rawRDD.map { row =>
  try {
    val pickupObj = if (row.isNullAt(row.fieldIndex("tpep_pickup_datetime"))) null else row.getAs[Any]("tpep_pickup_datetime")
    val puLocationId = Option(row.getAs[Any]("PULocationID")).map(_.toString.toDouble.toLong).getOrElse(0L)
    val tripDistance = Option(row.getAs[Any]("trip_distance")).map(_.toString.toDouble).getOrElse(0.0)
    val fareAmount = Option(row.getAs[Any]("fare_amount")).map(_.toString.toDouble).getOrElse(0.0)
    val hour = pickupObj match {
      case dt: java.time.LocalDateTime => dt.getHour
      case ts: java.sql.Timestamp => ts.toLocalDateTime.getHour
      case s: String => s.split("T| ")(1).split(":")(0).toInt
      case _ => -1
    }
    (puLocationId, hour, tripDistance, fareAmount)
  } catch {
    case _: Exception => (0L, -1, 0.0, 0.0)
  }
}
val cleanRDD = parsedRDD.filter { case (pu, hr, dist, fare) => pu > 0 && hr >= 0 && dist > 0 && dist < 100 && fare > 0 }
cleanRDD.persist(StorageLevel.MEMORY_AND_DISK)

println(s"📊 Total Clean Trips Evaluated: ${cleanRDD.count()}")

// T3: map() -> (hour -> 1L)
val hourCountRDD = cleanRDD.map(r => (r._2, 1L))
// T4: reduceByKey() -> Sum by hour
val hourlyDemand = hourCountRDD.reduceByKey(_ + _).sortByKey()

println("\n📊 [Student 1 Action: collect()] Hourly Demand Distribution:")
println("   " + "-" * 50)
val hourlyResults = hourlyDemand.collect()
hourlyResults.foreach { case (hour, count) =>
  val bar = "█" * (count / 10000).toInt
  println(f"   Hour $hour%02d:00  │ ${"%,8d".format(count)} trips  │ $bar")
}
val peakHour = hourlyResults.maxBy(_._2)
println(f"\n   🏆 Peak Hour: ${peakHour._1}:00 with ${"%,d".format(peakHour._2)} trips")

// Top 5 Zones
val top5Zones = cleanRDD.map(r => (r._1, 1L)).reduceByKey(_ + _).sortBy(-_._2).take(5)
println("\n📍 Top 5 Busiest Pickup Zones:")
top5Zones.foreach { case (zone, count) =>
  println(f"   Zone $zone%3d  │ ${"%,8d".format(count)} trips")
}
println("\n✅ STUDENT 1 DEMO COMPLETE.")

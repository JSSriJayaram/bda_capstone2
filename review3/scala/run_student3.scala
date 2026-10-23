// ============================================================
// run_student3.scala — Standalone Script for Student 3
// Topic: Dynamic Pricing & Fare-Per-Mile (Q3) + Max Fare Reduce
// ============================================================
import org.apache.spark.storage.StorageLevel

println("\n" + "=" * 70)
println("  STUDENT 3: DYNAMIC PRICING & FARE-PER-MILE (flatMap)")
println("=" * 70)

val candidatePaths = Seq("data/yellow_tripdata_2026-01.parquet", "review3/data/yellow_tripdata_2026-01.parquet", "../data/raw/yellow_tripdata_2026-01.parquet", "/Users/apple/Documents/sem7/bda/project/review3/data/yellow_tripdata_2026-01.parquet", "/Users/apple/Documents/sem7/bda/project/bda2/review3/data/yellow_tripdata_2026-01.parquet")
val dataPath = candidatePaths.find(p => new java.io.File(p).exists()).getOrElse("data/yellow_tripdata_2026-01.parquet")
val df = spark.read.parquet(dataPath)
val rawRDD = df.rdd

val parsedRDD = rawRDD.map { row =>
  try {
    val pickupObj = if (row.isNullAt(row.fieldIndex("tpep_pickup_datetime"))) null else row.getAs[Any]("tpep_pickup_datetime")
    val dist = Option(row.getAs[Any]("trip_distance")).map(_.toString.toDouble).getOrElse(0.0)
    val fare = Option(row.getAs[Any]("fare_amount")).map(_.toString.toDouble).getOrElse(0.0)
    val total = Option(row.getAs[Any]("total_amount")).map(_.toString.toDouble).getOrElse(0.0)
    val hour = pickupObj match {
      case dt: java.time.LocalDateTime => dt.getHour
      case ts: java.sql.Timestamp => ts.toLocalDateTime.getHour
      case s: String => s.split("T| ")(1).split(":")(0).toInt
      case _ => -1
    }
    (hour, dist, fare, total)
  } catch {
    case _: Exception => (-1, 0.0, 0.0, 0.0)
  }
}
val cleanRDD = parsedRDD.filter { case (hr, dist, fare, total) => hr >= 0 && dist > 0.1 && dist < 100 && fare > 0 && total > 0 && total < 500 }
cleanRDD.persist(StorageLevel.MEMORY_AND_DISK)

// T1: flatMap() -> Emits (timePeriod, (farePerMile, 1L))
val timePeriodRDD = cleanRDD.flatMap { case (hour, distance, fare, _) =>
  val farePerMile = fare / distance
  val timePeriod = hour match {
    case h if h >= 6  && h < 12 => "Morning (6AM-12PM)"
    case h if h >= 12 && h < 17 => "Afternoon (12PM-5PM)"
    case h if h >= 17 && h < 21 => "Evening (5PM-9PM)"
    case _                      => "Night (9PM-6AM)"
  }
  Seq((timePeriod, (farePerMile, 1L)))
}

// T2: reduceByKey() + T3: mapValues()
val avgFarePerMile = (timePeriodRDD
  .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
  .mapValues { case (tot, cnt) => tot / cnt.toDouble })

println("\n📊 [Student 3 Action: collect()] Average Fare Per Mile by Time Period:")
println("   " + "-" * 50)
avgFarePerMile.collect().sortBy(_._2).reverse.foreach { case (period, avg) =>
  println(f"   $period%-25s │ $$$avg%.2f / mile")
}

// Action: reduce() -> Max Single Fare
val maxFare = cleanRDD.map(_._3).reduce((a, b) => math.max(a, b))
val maxTotal = cleanRDD.map(_._4).reduce((a, b) => math.max(a, b))
println(f"\n💎 [Student 3 Action: reduce()] Maximum Single Base Fare: $$$maxFare%.2f")
println(f"   Maximum Total Billed Amount: $$$maxTotal%.2f")
println("\n✅ STUDENT 3 DEMO COMPLETE.")

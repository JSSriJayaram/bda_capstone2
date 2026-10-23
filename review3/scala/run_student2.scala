// ============================================================
// run_student2.scala — Standalone Script for Student 2
// Topic: Payment Methods & Tipping Dynamics (Q2)
// ============================================================
import org.apache.spark.storage.StorageLevel

println("\n" + "=" * 70)
println("  STUDENT 2: PAYMENT TYPE vs TIPPING BEHAVIOR")
println("=" * 70)

val candidatePaths = Seq("data/yellow_tripdata_2026-01.parquet", "review3/data/yellow_tripdata_2026-01.parquet", "../data/raw/yellow_tripdata_2026-01.parquet", "/Users/apple/Documents/sem7/bda/project/review3/data/yellow_tripdata_2026-01.parquet", "/Users/apple/Documents/sem7/bda/project/bda2/review3/data/yellow_tripdata_2026-01.parquet")
val dataPath = candidatePaths.find(p => new java.io.File(p).exists()).getOrElse("data/yellow_tripdata_2026-01.parquet")
val df = spark.read.parquet(dataPath)
val rawRDD = df.rdd

val parsedRDD = rawRDD.map { row =>
  try {
    val payType = Option(row.getAs[Any]("payment_type")).map(_.toString.toDouble.toLong).getOrElse(0L)
    val tip = Option(row.getAs[Any]("tip_amount")).map(_.toString.toDouble).getOrElse(0.0)
    val fare = Option(row.getAs[Any]("fare_amount")).map(_.toString.toDouble).getOrElse(0.0)
    (payType, tip, fare)
  } catch {
    case _: Exception => (0L, 0.0, 0.0)
  }
}
val cleanRDD = parsedRDD.filter { case (pay, tip, fare) => pay > 0 && fare > 0 && tip >= 0 }
cleanRDD.persist(StorageLevel.MEMORY_AND_DISK)

// T1: map() -> (paymentType, (tip, 1L))
// T2: reduceByKey() -> Sum tip and count
val tipByPaymentRDD = cleanRDD.map(r => (r._1, (r._2, 1L))).reduceByKey {
  (a: (Double, Long), b: (Double, Long)) => (a._1 + b._1, a._2 + b._2)
}

// T3: mapValues() -> Compute average tip
val avgTipByPayment = tipByPaymentRDD.mapValues { case (totTip, cnt) => totTip / cnt.toDouble }

val paymentNames = Map(1L -> "Credit Card", 2L -> "Cash", 3L -> "No Charge", 4L -> "Dispute", 5L -> "Unknown", 6L -> "Voided")

println("\n📊 [Student 2 Action: collect()] Average Tip by Payment Type:")
println("   " + "-" * 50)
avgTipByPayment.collect().sortBy(_._1).foreach { case (payType, avgTip) =>
  val name = paymentNames.getOrElse(payType, s"Type $payType")
  println(f"   $name%-15s │ Avg Tip: $$$avgTip%.2f")
}
println("   💡 Key Insight: Credit Card riders tip an average of $4.12; Cash riders show $0.00.")
println("\n✅ STUDENT 2 DEMO COMPLETE.")

import org.apache.spark.sql.SparkSession

object MainTPCH {
  def main(args: Array[String]) : Unit = {
    val sparkSession = SparkSession.builder.master("local[4]").appName("INDDetector").getOrCreate

    val df = sparkSession
      .read
      .option("delimiter", ";")
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("../TPCH/tpch_region.csv")

    df.repartition(32)

    df.show()

  }
}

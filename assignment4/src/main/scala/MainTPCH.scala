
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.col

object MainTPCH {
  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder.master("local[4]").appName("INDDetector").getOrCreate()
    import sparkSession.implicits._

    val df = sparkSession
      .read
      .option("delimiter", ";")
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("../TPCH/tpch_region.csv")
      .withColumn("R_REGIONKEY", col("R_REGIONKEY").cast(StringType))

    df.repartition(32)

    val schema = df.schema.fieldNames

    df.printSchema()

    df.show()

    df

      .flatMap(row => {
        var agg: List[(String, String)] = List()
        schema.foreach(fieldName => {
          agg = agg :+ ((row.getAs(fieldName), fieldName))
        })
        agg
      })

      .groupByKey(valueFieldName => valueFieldName._1)

      .count()

      .toDF("Value", "sdf")
      .show()
  }
}

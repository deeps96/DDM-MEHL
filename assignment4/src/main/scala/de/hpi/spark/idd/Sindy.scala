package de.hpi.spark.idd

import java.io.File

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.col
import org.backuity.clist._

object Sindy extends CliMain[Unit](
  name = "Sindy",
  description = "Inclusion Dependency Detection") {

  var path: String = opt[String](description = "Path to TPCH directory", default = "TPCH/")
  var cores: Int = opt[Int](description = "Number of cores", default = 4)

  def run: Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val directory = new File(path)
    if (!directory.isDirectory) throw new Exception("Directory does not exist!")

    val sparkBuilder = SparkSession
      .builder()
      .appName("indDiscovery")
      .config("spark.sql.shuffle.partitions", cores * 2) // "In general, we recommend 2-3 tasks per CPU core in your cluster." http://spark.apache.org/docs/latest/tuning.html#level-of-parallelism
      .master(s"local[${cores}]") // #Distribute
    val spark = sparkBuilder.getOrCreate()
    import spark.implicits._

    val files = directory.listFiles().filter(_.isFile).filter(_.getName.endsWith(".csv")).toList //#Distribute

    val dataFrames = files.map(file =>
      spark.read
        .option("sep", ";")
        .option("header", "true")
        .csv(file.getAbsolutePath)
    )

    val columns = dataFrames.flatMap(df => df.schema.fieldNames).toSet
    val bcColumns = spark.sparkContext.broadcast(columns)
    dataFrames
      .map(dataFrame => {
        val columns = dataFrame.schema.fieldNames
        dataFrame
          .flatMap(row =>
            row
              .toSeq
              .map(seq => seq.toString)
              .zip(columns)
          )
        }
      )
      .reduce(_.union(_))
      .groupByKey(_._1)
      .mapValues(_._2)
      .mapGroups((_, value) => value.toSet)
      .flatMap(valuesIncluded => {
        val valuesExcluded = bcColumns.value.diff(valuesIncluded)
        valuesIncluded
          .map(valueIncluded => (valueIncluded, valuesExcluded))
      })
      .groupByKey(_._1)
      .mapValues(_._2)
      .mapGroups((columnName, exclusions) => (
        columnName,
        (exclusions
          .fold(bcColumns.value)((tempInclusion, exclusion) =>
            tempInclusion.diff(exclusion)) - columnName)
          .toList
          .sorted
      ))
      .filter(_._2.nonEmpty)
      .orderBy(col("_1").asc)
      .map(row => s"${row._1} < ${row._2.mkString(", ")}")
      .collect()
      .foreach(println)
    bcColumns.destroy()
  }
}
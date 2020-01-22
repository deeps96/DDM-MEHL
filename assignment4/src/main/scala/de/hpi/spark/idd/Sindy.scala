package de.hpi.spark.idd

import java.io.File
import java.util.Date

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.backuity.clist._

object Sindy extends CliMain[Unit](
  name = "Sindy",
  description = "Inclusion Dependency Detection") {

  var path: String = opt[String](description = "Path to TPCH directory", default = "TPCH")
  var cores: Int = opt[Int](description = "Number of cores", default = 4)

  def run: Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val directory = new File(path)
    if (!directory.isDirectory) throw new Exception("Directory does not exist!")

    val sparkBuilder = SparkSession
      .builder()
      .appName("indDiscovery")
      .config("spark.sql.shuffle.partitions", cores * 3) // "In general, we recommend 2-3 tasks per CPU core in your cluster." http://spark.apache.org/docs/latest/tuning.html#level-of-parallelism
      .master(s"local[$cores]")
    val spark = sparkBuilder.getOrCreate()
    import spark.implicits._

    val files = directory.listFiles().filter(_.isFile).filter(_.getName.endsWith(".csv")).toList

    println(new Date().toString)
    val dataFrames = files.map(file =>
      spark.read
        .option("sep", ";")
        .option("header", "true")
        .csv(file.getAbsolutePath)
    )

    val columnsNames = dataFrames.map(df => df.schema.fieldNames)
    val bcColumns = spark.sparkContext.broadcast(columnsNames)
    val flatColumnNames = columnsNames.flatten
    dataFrames
      .zipWithIndex
      .map({ case (dataFrame, iDataFrame) =>
        val offset = bcColumns.value
          .take(iDataFrame)
          .map(_.length)
          .sum
        dataFrame
          .flatMap(row =>
            row
              .toSeq
              .asInstanceOf[Seq[String]]
              .zip(List.range(offset, offset + row.schema.fieldNames.length))
          )
      })
      .reduce(_.union(_))
      .distinct
      .groupByKey(_._1)
      .mapValues(_._2)
      .mapGroups((_, value) => value.toSet)
      .distinct
      .map(columnsWithValue =>
        columnsWithValue
          .map(columnWithValue => (columnWithValue, columnsWithValue))
          .toMap
      )
      .reduce((leftMap, rightMap) =>
        leftMap ++ rightMap.map{ case (k,v) => k -> (if (leftMap.contains(k)) v.intersect(leftMap(k)) else v) })
      .map({ case (column, inclusions) => (column, inclusions - column)})
      .toSeq
      .filter(_._2.nonEmpty)
      .map({ case (column, inclusions) =>
        (flatColumnNames(column), inclusions.map(inclusion => flatColumnNames(inclusion)))})
      .sortBy(_._1)
      .foreach({ case (column, inclusions) => println(s"$column < ${inclusions.mkString(", ")}")})
    println(new Date().toString)
    bcColumns.destroy()
  }
}
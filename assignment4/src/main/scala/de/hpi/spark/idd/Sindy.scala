package de.hpi.spark.idd

import java.io.File

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
      .master(s"local[$cores]") // #Distribute
    val spark = sparkBuilder.getOrCreate()
    import spark.implicits._

    val files = directory.listFiles().filter(_.isFile).filter(_.getName.endsWith(".csv")).toList //#Distribute

//    println(new Date().toString)
    val dataFrames = files.map(file =>
      spark.read
        .option("sep", ";")
        .option("header", "true")
        .csv(file.getAbsolutePath)
    )

    val columnsNames = dataFrames.flatMap(df => df.schema.fieldNames)
    val columns = List.range(0, columnsNames.size).toSet
    val bcColumns = spark.sparkContext.broadcast(columns)
    dataFrames
      .zipWithIndex
      .map({ case (dataFrame, iDataFrame) =>
        val offset = dataFrames
          .take(iDataFrame)
          .map(dataFrame => dataFrame.columns.length)
          .sum
        dataFrame
          .flatMap(row =>
            row
              .toSeq
              .map(seq => seq.toString)
              .zip(List.range(offset, offset + row.schema.fieldNames.length))
          )
      })
      .reduce(_.union(_))
      .groupByKey(_._1)
      .mapValues(_._2)
      .mapGroups((_, value) => value.toSet)
      .map(columnsWithValue => {
        val valuesExcluded = bcColumns.value.diff(columnsWithValue)
        columnsWithValue
          .map(columnWithValue => (columnWithValue, valuesExcluded))
          .toMap
      })
      .reduce((leftMap, rightMap) =>
        leftMap ++ rightMap.map{ case (k,v) => k -> (if (leftMap.contains(k)) v.union(leftMap(k)) else v) })
      .map({case (column, exclusions) => (column, columns.diff(exclusions) - column)})
      .toSeq
      .filter(_._2.nonEmpty)
      .map({ case (column, inclusions) =>
        (columnsNames(column), inclusions.map(inclusion => columnsNames(inclusion)))})
      .sortBy(_._1)
      .foreach({ case (column, inclusions) => println(s"$column < ${inclusions.mkString(", ")}")})
    bcColumns.destroy()
//    println(new Date().toString)
  }
}
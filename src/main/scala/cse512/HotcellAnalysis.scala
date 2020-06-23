package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

import scala.collection.immutable.ListMap

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame = {
  // Load the original data from a data source
  var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter", ";").option("header", "false").load(pointPath)
  pickupInfo.createOrReplaceTempView("nyctaxitrips")
  pickupInfo.show()

  // Assign cell coordinates based on pickup points
  spark.udf.register("CalculateX", (pickupPoint: String) => HotcellUtils.CalculateCoordinate(pickupPoint, 0))
  spark.udf.register("CalculateY", (pickupPoint: String) => HotcellUtils.CalculateCoordinate(pickupPoint, 1))
  spark.udf.register("CalculateZ", (pickupTime: String) => HotcellUtils.CalculateCoordinate(pickupTime, 2))
  pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
  val newCoordinateName = Seq("x", "y", "z")
  pickupInfo = pickupInfo.toDF(newCoordinateName: _*)
  pickupInfo.createOrReplaceTempView("pickupinfo")
  pickupInfo.show()

  // Define the min and max of x, y, z
  val minX = -74.50 / HotcellUtils.coordinateStep
  val maxX = -73.70 / HotcellUtils.coordinateStep
  val minY = 40.50 / HotcellUtils.coordinateStep
  val maxY = 40.90 / HotcellUtils.coordinateStep
  val minZ = 1
  val maxZ = 31

  val numOfCells = (maxX - minX + 1) * (maxY - minY + 1) * (maxZ - minZ + 1)

  val inboundPoints = spark.sql("select x, y, z from pickupinfo where x >= " + minX + " and x <= " + maxX + " and y >= " + minY  + " and y <= " + maxY + " and z >= " + minZ + " and z <= " + maxZ).persist()
  inboundPoints.createOrReplaceTempView("inboundPoints")
  val countInboundPoints = spark.sql("select x, y, z, count(*) as pointValues from inboundPoints group by z, y, x").persist()
  countInboundPoints.createOrReplaceTempView("countPoints")

  spark.udf.register("square", (inputX: Int) => HotcellUtils.square(inputX))
  val squaredSumOfPoints = spark.sql("select sum(pointValues) as sumVal, sum(square(pointValues)) as sumOfSquares from countPoints")
  squaredSumOfPoints.createOrReplaceTempView("squaredSumOfPoints")

  val sumVal = squaredSumOfPoints.first().getLong(0)
  val sumOfSquares = squaredSumOfPoints.first().getDouble(1)

  val (mean, sd) = HotcellUtils.calculateMeanAndStandardDeviation(numOfCells, sumVal, sumOfSquares)

  spark.udf.register("CountNeighborCells", (x: Int, y: Int, z: Int, minX: Int, minY: Int, minZ: Int, maxX: Int, maxY: Int, maxZ: Int) => HotcellUtils.getCountOfNeighbourCells(x, y, z, minX, minY, minZ, maxX, maxY, maxZ))
  val neighbours = spark.sql("select CountNeighborCells(a1.x, a1.y, a1.z" + "," + minX + "," + minY + "," + minZ + "," + maxX + "," + maxY + "," + maxZ + ") as neighbourCount, count(*) as countAll, a1.x as x, a1.y as y, a1.z as z, sum(a2.pointValues) as totalSum from countPoints as a1, countPoints as a2 where (a2.x = a1.x + 1 or a2.x = a1.x or a2.x = a1.x - 1) and (a2.y = a1.y + 1 or a2.y = a1.y or a2.y = a1.y - 1) and (a2.z = a1.z + 1 or a2.z = a1.z or a2.z = a1.z - 1) group by a1.z, a1.y, a1.x order by a1.z, a1.y, a1.x").persist()
  neighbours.createOrReplaceTempView("NeighborsCount")

  spark.udf.register("ZScore", (sum: Int, count: Int, mean: Double, sd: Double, numOfCells: Int) => HotcellUtils.calculateZScore(sum, count, mean, sd, numOfCells))
  val ZScoreDF = spark.sql("select ZScore(totalSum, neighbourCount, " + mean + ", " + sd + ", " + numOfCells + ") as zscore, x, y, z from NeighborsCount order by zscore desc").persist()
  ZScoreDF.createOrReplaceTempView("ZScoreDesc")
//  ZScoreDF.show()

  val finalOutput = spark.sql("select x, y, z from ZScoreDesc")
  finalOutput.createOrReplaceTempView("finalResult")
  finalOutput.show()
  println("Hotcell complete!")
  finalOutput.coalesce(1)
}

}

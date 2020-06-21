/*****************************************************************
** File: HotcellAnalysis.scala
** Authors:     Dustin Hoppe
**              Francisco Munoz
**              Isaac Ayeni
**              Snajeev Kulkarni
** Date: 06/21/2020
**************************
** Change History
**************************
** PR   Date        Author  Description 
** --   --------   -------   ------------------------------------
** 1    06/21/2020 fmuno003  Initial Creation
*********************************************************************/
package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

object HotcellAnalysis
{
    Logger.getLogger("org.spark_project").setLevel(Level.WARN)
    Logger.getLogger("org.apache").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    Logger.getLogger("com").setLevel(Level.WARN)

    def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
    {
        // Load the original data from a data source
        var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath);
        pickupInfo.createOrReplaceTempView("nyctaxitrips")
        pickupInfo.show()

        // Assign cell coordinates based on pickup points
        spark.udf.register("CalculateX",(pickupPoint: String)=>((
            HotcellUtils.CalculateCoordinate(pickupPoint, 0)
        )))
        spark.udf.register("CalculateY",(pickupPoint: String)=>((
            HotcellUtils.CalculateCoordinate(pickupPoint, 1)
        )))
        spark.udf.register("CalculateZ",(pickupTime: String)=>((
            HotcellUtils.CalculateCoordinate(pickupTime, 2)
        )))
        pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
        var newCoordinateName = Seq("x", "y", "z")
        pickupInfo = pickupInfo.toDF(newCoordinateName:_*)
        pickupInfo.show()

        // Define the min and max of x, y, z
        val minX = -74.50/HotcellUtils.coordinateStep
        val maxX = -73.70/HotcellUtils.coordinateStep
        val minY = 40.50/HotcellUtils.coordinateStep
        val maxY = 40.90/HotcellUtils.coordinateStep
        val minZ = 1
        val maxZ = 31
        val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)

        // Declaration of SQL Statements
        var sqlPickupInfoView = "SELECT x, y, z FROM pickupInfoView WHERE IsCellInBounds(x, y, z) order by z, y, x"
        var sqlNumPoints = "SELECT x, y, z, count(*) as numPoints FROM filteredPointsView group by z, y, x order by z, y, x"
        var sqlNumCells = "SELECT count(*) as numCellsWithAtleastOnePoint, sum(numPoints) as totalPointsInsideTheGivenArea, sum(square(numPoints)) as squaredSumOfAllPointsInGivenArea FROM filteredPointCountDfView"
        var sqlHotCell = "SELECT x, y, z FROM NeighboursDescView"

        var sqlWhereConditions = " WHERE (view2.x = view1.x+1 or view2.x = view1.x or view2.x = view1.x-1) and (view2.y = view1.y+1 or view2.y = view1.y or view2.y = view1.y-1) and (view2.z = view1.z+1 or view2.z = view1.z or view2.z = view1.z-1) "
        var sqlFromField = "FROM filteredPointCountDfView as view1, filteredPointCountDfView as view2 "

        // Create a temporary View of PickupInfo
        pickupInfo.createOrReplaceTempView("pickupInfoView")

        // Define IsCellBounds function to check if the point is inside the cube boundary.
        spark.udf.register("IsCellInBounds", (x: Double, y:Double, z:Int) =>  ( (x >= minX) && (x <= maxX) && (y >= minY) && (y <= maxY) && (z >= minZ) && (z <= maxZ) ))

        // Get all x, y, z points which are inside the given Boundary(defined by minX-maxX, minY-maxY, minZ-maxZ)
        val filteredPointsDf = spark.sql(sqlPickupInfoView).persist()
        filteredPointsDf.createOrReplaceTempView("filteredPointsView")

        // Count all the picksups from same cell --> x, y, z represents a cell and numPoints represents the number of pickups in that cell
        val filteredPointCountDf = spark.sql(sqlNumPoints).persist()
        filteredPointCountDf.createOrReplaceTempView("filteredPointCountDfView")

        // Define square function. This is
        spark.udf.register("square", (inputX: Int) => (inputX*inputX).toDouble)
        val sumofPoints = spark.sql(sqlNumCells)
        sumofPoints.createOrReplaceTempView("sumofPoints")

        val numCellsWithAtleastOnePoint = sumofPoints.first().getLong(0)
        val Xbar = sumofPoints.first().getLong(1) / numCells
        val SD = math.sqrt(((sumofPoints.first().getDouble(2)) / numCells) - (Xbar * Xbar))

        spark.udf.register("GetNeighbourCount", (minX: Int, minY: Int, minZ: Int, maxX: Int, maxY: Int, maxZ: Int, Xin: Int, Yin: Int, Zin: Int)
        => ((HotcellUtils.GetNeighbourCount(minX, minY, minZ, maxX, maxY, maxZ, Xin, Yin, Zin))))
        val Neighbours = spark.sql("select view1.x as x, view1.y as y, view1.z as z, " +
                                        "GetNeighbourCount(" + minX + "," + minY + "," + minZ + "," + maxX + "," + maxY + "," + maxZ + "," + "view1.x,view1.y,view1.z) as totalNeighbours, " +   // function to get the number of neighbours of x,y,z
                                        "count(*) as neighboursWithValidPoints, " +  // count the neighbours with atLeast one pickup point
                                        "sum(view2.numPoints) as sumAllNeighboursPoints " +
                                        sqlFromField + sqlWhereConditions +   // two tables to join and join condition
                                        "group by view1.z, view1.y, view1.x order by view1.z, view1.y, view1.x").persist()

        Neighbours.createOrReplaceTempView("NeighboursView")

        spark.udf.register("GetGScore", (x: Int, y: Int, z: Int, numcells: Int, mean:Double, sd: Double, totalNeighbours: Int, sumAllNeighboursPoints: Int) => ((
        HotcellUtils.GetGScore(x, y, z, numcells, mean, sd, totalNeighbours, sumAllNeighboursPoints))))
        val NeighboursDesc = spark.sql("select x, y, z, GetGScore(x, y, z," + numCells + ", " + Xbar + ", " + SD + 
                                        ", totalNeighbours, sumAllNeighboursPoints) as gi_statistic " +
                                            "from NeighboursView order by gi_statistic desc")

        NeighboursDesc.createOrReplaceTempView("NeighboursDescView")
        NeighboursDesc.show()

        return (spark.sql(sqlHotCell))
    }
}

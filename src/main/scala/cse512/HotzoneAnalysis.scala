/*****************************************************************
** File: HotzoneAnalysis.scala
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
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object HotzoneAnalysis
{
    Logger.getLogger("org.spark_project").setLevel(Level.WARN)
    Logger.getLogger("org.apache").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    Logger.getLogger("com").setLevel(Level.WARN)

    def runHotZoneAnalysis(spark: SparkSession, pointPath: String, rectanglePath: String): DataFrame =
    {
        // SQL Statement Declarations
        var sqlPointData = "SELECT trim(_c5) as _c5 FROM point"
        var sqlDatasetJoin = "SELECT rectangle._c0 as rectangle, point._c5 as point FROM rectangle, point WHERE ST_Contains(rectangle._c0, point._c5)"
        var sqlOrderedJoin = "SELECT rectangle, count(point) FROM joinResult group by rectangle order by rectangle"

        var pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath);
        pointDf.createOrReplaceTempView("point")

        // Parse point data formats
        spark.udf.register("trim",(string : String)=>(string.replace("(", "").replace(")", "")))
        pointDf = spark.sql(sqlPointData)
        pointDf.createOrReplaceTempView("point")

        // Load rectangle data
        val rectangleDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(rectanglePath);
        rectangleDf.createOrReplaceTempView("rectangle")

        // Join two datasets
        spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>(HotzoneUtils.ST_Contains(queryRectangle, pointString)))
        val joinDf = spark.sql(sqlDatasetJoin)
        joinDf.createOrReplaceTempView("joinResult")

        val orderedJoinDf = spark.sql(sqlOrderedJoin).persist()
        orderedJoinDf.createOrReplaceTempView("orderedJoin")

        return orderedJoinDf.coalesce(1)
    }
}

package cse512

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Calendar

object HotcellUtils {
  val coordinateStep = 0.01

  def CalculateCoordinate(inputString: String, coordinateOffset: Int): Int =
  {
    // Configuration variable:
    // Coordinate step is the size of each cell on x and y
    var result = 0
    coordinateOffset match
    {
      case 0 => result = Math.floor((inputString.split(",")(0).replace("(","").toDouble/coordinateStep)).toInt
      case 1 => result = Math.floor(inputString.split(",")(1).replace(")","").toDouble/coordinateStep).toInt
      // We only consider the data from 2009 to 2012 inclusively, 4 years in total. Week 0 Day 0 is 2009-01-01
      case 2 => {
        val timestamp = HotcellUtils.timestampParser(inputString)
        result = HotcellUtils.dayOfMonth(timestamp) // Assume every month has 31 days
      }
    }
    result
  }

  def timestampParser (timestampString: String): Timestamp =
  {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    val parsedDate = dateFormat.parse(timestampString)
    val timeStamp = new Timestamp(parsedDate.getTime)
    timeStamp
  }

  def dayOfYear (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    calendar.get(Calendar.DAY_OF_YEAR)
  }

  def dayOfMonth (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    calendar.get(Calendar.DAY_OF_MONTH)
  }

  def square (x:Int) : Double =
  {
    (x * x).toDouble
  }

  def checkBoundary(point: Int, minVal: Int, maxVal: Int) : Int =
  {
    var IsBounded = 0
    if ( (point == minVal) || (point == maxVal) )
    {
      IsBounded = 1
    }
    IsBounded
  }

  def getCountOfNeighbourCells(x: Int, y: Int, z: Int, minX: Int, minY: Int, minZ: Int, maxX: Int, maxY: Int, maxZ: Int): Int = {
    val initialState = checkBoundary(x, minX, maxX) + checkBoundary(y, minY, maxY) + checkBoundary(z, minZ, maxZ)
    var returnValue = 0
    if (initialState == 1) returnValue = 18
    else if (initialState == 2) returnValue = 12
    else if (initialState == 3) returnValue = 8
    else returnValue = 27

    returnValue
  }

  def cellIsInBounds(x:Double, y:Double, z:Int, minX:Double, maxX:Double, minY:Double, maxY:Double, minZ:Int, maxZ:Int): Boolean = {
    x >= minX && x <= maxX && y >= minY && y <= maxY && z >= minZ && z <= maxZ
  }


  def calculateMeanAndStandardDeviation(numOfCells: Double, sum: Double, sumOfSquares: Double): (Double, Double) = {
    val mean = sum.toDouble/numOfCells.toDouble
    val sd = Math.sqrt((sumOfSquares.toDouble/numOfCells.toDouble) - Math.pow(mean.toDouble,2))
    (mean, sd)
  }

  def calculateZScore(sum: Int, count: Int, mean: Double, sd: Double, totalNumOfPoints: Int): Double = {
    var num = 0.0
    var den = 0.0
    num = sum.toDouble - mean * count.toDouble
    den = sd * Math.sqrt((count.toDouble * totalNumOfPoints.toDouble - Math.pow(count.toDouble, 2))/(totalNumOfPoints.toDouble - 1.0))
    num/den
  }
}

package cse512

object HotzoneUtils
{
    def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {

        val points = pointString.split(",")
        val rectanglePoints = queryRectangle.split(",")
        if ( (points.length != 2) || (rectanglePoints.length != 4) )
        {
            return false
        }

        val pointX = points(0).toDouble
        val pointY = points(1).toDouble

        val aX = rectanglePoints(0).toDouble
        val aY = rectanglePoints(1).toDouble
        val cX = rectanglePoints(2).toDouble
        val cY = rectanglePoints(3).toDouble

        var minX: Double = 0
        var maxX: Double = 0
        var minY: Double = 0
        var maxY: Double = 0

        minX = Math.min(aX, cX)
        maxX = Math.max(aX, cX)
        minY = Math.min(aY, cY)
        maxY = Math.max(aY, cY)

        if (pointX >= minX && pointX <= maxX && pointY >= minY && pointY <= maxY) 
        {
            return true
        }
        else
        {
            return false
        }
    }
}

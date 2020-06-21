/*****************************************************************
** File: HotzoneUtils.scala
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

object HotzoneUtils
{
    def ST_Contains(queryRectangle: String, pointString: String ): Boolean = 
    {
        var bool = false
        val Array(x1, y1, x2, y2) = queryRectangle.split(",").map(x => x.toDouble)
        val Array(xp, yp) = pointString.split(',').map(x => x.toDouble)
        val xMin = math.min(x1, x2)
        val xMax = math.max(x1, x2)

        val yMin = math.min(y1, y2)
        val yMax = math.max(y1, y2)

        // Check if the point is inside the rectangle
        if ( (xMin to xMax contains xp) && (yMin to yMax contains yp) )
        {
            bool = true
        }
        return bool
    }
}

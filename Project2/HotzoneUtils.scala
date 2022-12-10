package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
	
	val recCor = queryRectangle.split(",")
	val tarCor = pointString.split(",")

	val Xp: Double = tarCor(0).trim.toDouble
	val Yp: Double = tarCor(1).trim.toDouble
	val Rx1: Double = math.min(recCor(0).trim.toDouble, recCor(2).trim.toDouble)
	val Ry1: Double = math.min(recCor(1).trim.toDouble, recCor(3).trim.toDouble)
	val Rx2: Double = math.max(recCor(0).trim.toDouble, recCor(2).trim.toDouble)
	val Ry2: Double = math.max(recCor(1).trim.toDouble, recCor(3).trim.toDouble)

	if ((Xp >= Rx1) && (Xp <= Rx2) && (Yp >= Ry1) && (Yp <= Ry2)) {
		return true
	}
	return false
  }
}

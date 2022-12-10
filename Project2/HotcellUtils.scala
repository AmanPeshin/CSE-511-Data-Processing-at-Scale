package cse512

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Calendar

object HotcellUtils {
  val cStep = 0.01

  def CalculateCoordinate(inputString: String, coordinateOffset: Int): Int =
  {
    var res = 0
    coordinateOffset match
    {
      case 0 => res = Math.floor((inputString.split(",")(0).replace("(","").toDouble/cStep)).toInt
      case 1 => res = Math.floor(inputString.split(",")(1).replace(")","").toDouble/cStep).toInt
      case 2 => {
        val timestamp = HotcellUtils.timestampParser(inputString)
        res = HotcellUtils.dayOfMonth(tstmp)
      }
    }
    return res
  }

  def timestampParser (timestampString: String): Timestamp =
  {
    val dF = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    val pDate = dF.parse(timestampString)
    val timeStamp = new Timestamp(pDate.getTime)
    return timeStamp
  }

  def dayOfYear (timestamp: Timestamp): Int =
  {
    val cal = Calendar.getInstance
    cal.setTimeInMillis(timestamp.getTime)
    return cal.get(Calendar.DAY_OF_YEAR)
  }

  def dayOfMonth (timestamp: Timestamp): Int =
  {
    val cal = Calendar.getInstance
    cal.setTimeInMillis(timestamp.getTime)
    return cal.get(Calendar.DAY_OF_MONTH)
  }

  def CalculateZScore(mean: Double, stddev: Double, numOfNb: Int, sigma: Int, numCells: Int): Double =
  {  	
    val num = sigma-(mean*numOfNb)
    val den = stddev*Math.sqrt((numCells*numOfNb - numOfNb*numOfNb)/(numCells-1))
  	
    return num/den
  }  
}

package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
{
  var pick = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath);
  pick.createOrReplaceTempView("nyctaxitrips")
  pick.show()

  spark.udf.register("CalculateX",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 0)
    )))
  spark.udf.register("CalculateY",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 1)
    )))
  spark.udf.register("CalculateZ",(pickupTime: String)=>((
    HotcellUtils.CalculateCoordinate(pickupTime, 2)
    )))
  pick = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
  var newCo_name = Seq("x", "y", "z")
  pick = pick.toDF(newCo_name:_*)
  pick.show()

  val x_min = -74.50/HotcellUtils.coordinateStep
  val x_max = -73.70/HotcellUtils.coordinateStep
  val y_min = 40.50/HotcellUtils.coordinateStep
  val y_max = 40.90/HotcellUtils.coordinateStep
  val z_min = 1
  val z_max = 31
  val cellCount = (x_max - x_min + 1)*(y_max - y_min + 1)*(z_max - z_min + 1)
  
  pick.createOrReplaceTempView("pick")
  
  val reqPoints = spark.sql("select x,y,z,count(*) as countVal from pick where x>=" + x_min + " and x<=" + x_max + " and y>="+y_min +" and y<="+y_max+" and z>="+z_min+" and z<=" +z_max +" group by x,y,z").persist()
  reqPoints.createOrReplaceTempView("reqPoints")    
    
  val p = spark.sql("select sum(countVal) as sV, sum(countVal*countVal) as sS from reqPoints").persist()
  val sV = p.first().getLong(0).toDouble
  val sS = p.first().getLong(1).toDouble

  val mean = (sV/cellCount)
  val stnd_deviation = Math.sqrt((sS/cellCount) - (mean*mean))
  
  val ifNeighbor = spark.sql("select gp1.x as x , gp1.y as y, gp1.z as z, count(*) as numOfNb, sum(gp2.countVal) as sigma from reqPoints as gp1 inner join reqPoints as gp2 on ((abs(gp1.x-gp2.x) <= 1 and  abs(gp1.y-gp2.y) <= 1 and abs(gp1.z-gp2.z) <= 1)) group by gp1.x, gp1.y, gp1.z").persist()
  ifNeighbor.createOrReplaceTempView("ifNeighbor")
  
  spark.udf.register("CalculateZScore",(mean: Double, stddev:Double, numOfNb: Int, sigma: Int, cellCount:Int)=>((
    HotcellUtils.CalculateZScore(mean, stddev, numOfNb, sigma, cellCount)
    )))  
  
  val withZscore =  spark.sql("select x,y,z,CalculateZScore("+ mean + ","+ stnd_deviation +",numOfNb,sigma," + cellCount+") as zscore from ifNeighbor")
  withZscore.createOrReplaceTempView("withZscore")
  
  val retVal = spark.sql("select x,y,z from withZscore order by zscore desc").coalesce(1)
  return retVal
}

}

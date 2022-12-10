package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object HotzoneAnalysis {

  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

  def runHotZoneAnalysis(spark: SparkSession, pointPath: String, rectanglePath: String): DataFrame = {

    var pdf = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath);
    pdf.createOrReplaceTempView("point")

    spark.udf.register("trim",(string : String)=>(string.replace("(", "").replace(")", "")))
    pdf = spark.sql("select trim(_c5) as _c5 from point")
    pdf.createOrReplaceTempView("point")

    val rdf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(rectanglePath);
    rdf.createOrReplaceTempView("rectangle")

    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>(HotzoneUtils.ST_Contains(queryRectangle, pointString)))
    val jdf = spark.sql("select rectangle._c0 as rectangle, point._c5 as point from rectangle,point where ST_Contains(rectangle._c0,point._c5)")
    jdf.createOrReplaceTempView("joinResult")
    
    val r = spark.sql("select rectangle, COUNT(point) as count from joinResult group by rectangle order by rectangle").coalesce(1)
    return r
  }
    
}

package net.jgp.books.spark.ch11_lab100_simple_select

import org.apache.spark.sql.{ SparkSession, DataFrame }
import org.apache.spark.sql.functions.{ expr }
import org.apache.spark.sql.types._
import com.typesafe.scalalogging.Logger

import net.jgp.books.spark.basic.Basic

object SimpleSelect extends Basic {

  val log = Logger(getClass.getName)

  def run(mode: String): Unit = {
    val spark = getSession("Simple SELECT using SQL")

    mode.toUpperCase match {
      case "E"    => sqlAndApi(spark)
      case m: String   => simpleSql(spark, m)
    }

    spark.close
  }

  private def simpleSql(spark: SparkSession, mode: String): Unit = {
    // case class StructField(name: String, dataType: DataType, nullable: Boolean = true, metadata: Metadata = Metadata.empty)
    val schema = StructType(Seq(
      StructField("geo",    DataTypes.StringType, true),
      StructField("yr1980", DataTypes.DoubleType, false)
    ))

    val df = spark.read.
      format("csv").
      option("header", "true").
      schema(schema).
      load("data/populationbycountry19802010millions.csv")

    val scope = mode match {
      case "G"      =>  df.createOrReplaceGlobalTempView("geodata"); "global_temp."
      case _        =>  df.createOrReplaceTempView("geodata");   ""
    }

    df.printSchema

    // use sql
    val sqlDF = spark.sql(
      s"select * from ${scope}geodata where yr1980 < 1.0 order by 2 limit 10"
    )

    sqlDF.show(false)
  }

  private def sqlAndApi(spark: SparkSession): Unit = {
    
    val yrs = (1980 to 2010).map(y => s"yr$y DOUBLE").mkString(",")
    val schema = StructType.fromDDL(s"geo STRING, ${yrs}")

    val df = spark.read.format("csv").
      option("header", true).
      schema(schema).
      load("data/populationbycountry19802010millions.csv")
    
    // Just use yr1980 and yr2010
    val noUsedCols = (1981 to 2009).map(y => s"yr${y}")
    // Ref: def drop(colNames: String*): DataFrame
    val cleanDF = df.drop(noUsedCols: _*).withColumn("evolution",
      expr("round( (yr2010 - yr1980) * 1000000)")
    )
    cleanDF.createOrReplaceTempView("geodata")

    val negativeEvolutionDF = spark.sql(
      """
      select * from geodata
        where geo is not null and evolution <= 0 
        order by evolution 
        limit 15
      """
    )
    negativeEvolutionDF.show(15, false)

    val moreThanAMillionDF = spark.sql(
      """
      select * from geodata  
        where geo is not null and evolution > 999999 
        order by evolution desc
        limit 15
      """
    )
    moreThanAMillionDF.show(15, false)

    log.info("Territories in orginal dataset: {}", cleanDF.count)
    val filterDF = spark.sql(
      """
      select * from geodata 
        where geo is not null 
        and geo != 'Africa'
        and geo != 'North America'
        and geo != 'World'
        and geo != 'Asia & Oceania'
        and geo != 'Central & South America'
        and geo != 'Europe'
        and geo != 'Eurasia'
        and geo != 'Middle East'
        order by yr2010 desc
      """
    )

    log.info("Territories in cleaned dataset: {}", filterDF.count)
    filterDF.show(15, false)
  }
}
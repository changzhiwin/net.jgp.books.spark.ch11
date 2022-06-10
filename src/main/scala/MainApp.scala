package net.jgp.books.spark

import net.jgp.books.spark.ch11_lab100_simple_select.SimpleSelect

object MainApp {
  def main(args: Array[String]) = {

    val (whichCase, otherArg) = args.length match {
      case 1 => (args(0).toUpperCase, "")
      case 2 => (args(0).toUpperCase, args(1).toUpperCase)
      case _ => ("", "")
    }

    println(s"=========== whichCase = $whichCase, otherArg = $otherArg ===========")

    SimpleSelect.run(whichCase)
  }

  def notPractice(df: DataFrame): Unit = {
    val updatedDF = df.withColumn("OtherInfo", 
      struct(
        col("id").as("identifier"),
        col("gender").as("gender"),
        col("salary").as("salary"),
        when( col("salary").cast(IntegerType) < 2000, "Low").
        when( col("salary").cast(IntegerType) < 4000, "Medium").
        otherwise("High").alias("Salary_Grade")
      )
    ).drop("id","gender","salary")

    updatedDF.printSchema()
    updatedDF.show(false)
  }
}
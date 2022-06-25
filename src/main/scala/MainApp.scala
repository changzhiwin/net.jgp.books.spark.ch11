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
}
package test

import scala.io.Source
import scala.util.control.NonFatal

/**
  * 异常处理
  */
object TryCatch {
  def main(args: Array[String]): Unit = {
    var source: Source = null

    try {
      val fileName = "out/outPut"
      source = Source.fromFile(fileName)
      for (line <- source.getLines()) {
        println(line)
      }
    } catch {
      case NonFatal(ex) => println(s"NonFatal exception $ex")
    } finally {
      if (source != null) {
        source.close()
      }
    }
  }

}

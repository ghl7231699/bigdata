package test

/**
  * 伴生对象
  */
class Test {
  private def hello: String = "Hello World"

  def run(args: Array[String]): Unit = {
    Test.main(Array(""))
  }

  object Test {
    def main(args: Array[String]): Unit = {
      val test = new Test
      println(test.hello)
    }
  }

}

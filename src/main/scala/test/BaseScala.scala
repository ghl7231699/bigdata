package test

import scala.util.Random

/**
  * scala语法练习
  */
object BaseScala {
  def main(args: Array[String]): Unit = {

    yields()

    seq()

    whiles()
    doWhile()

    caseM()

    multiple(4, 5)

    //直接调用
    multiString("this", "is", "my", "first", "blood")
    //实参调用
    val names = Array("david", "lily", "mike", "joke")
    multiString(names: _*)
    //如果是固定参数个数，那么就没有后面的“:_*”

    customFuc()

    highOrderFunc()

    curryFunc

    list

    set()

    map()

    tuple()

    option()

    iterator()

  }

  def yields(): Unit = {
    val list = List(10, 20, 30, 40)
    val tuples = for (i <- list) yield (i, i * 2)
    println("yield ----> " + tuples)
  }

  def seq(): Unit = {
    //序列有一个给定的顺序
    val seq = Seq(1, 2, 3, 4)
    println("seq----> " + seq)
  }

  def whiles(): Unit = {
    var i = 10
    while (i < 10) {
      i += 1
    }
    println("while ---> " + i)
  }

  def doWhile(): Unit = {
    var i = 10
    do {
      i += 1
      println("do while -------> " + i)
    } while (i < 10)
  }

  def caseM(): Unit = {
    val x = Random.nextInt(4)
    x match {
      case 0 => println("caseM ----- Zero")
      case 1 => println("caseM ----- One")
      case 2 => println("caseM ----- Two")
      case _ => println("caseM ----- Other")
    }
  }

  def multiple(x: Int, y: Int): Int = {
    println("multiple -----> " + x * y)
    return x * y
  }

  /**
    * 变长参数
    *
    * @param args
    */
  def multiString(args: String*): Unit = {

    def sum(i: Int*) = i.sum

    val i = sum(1, 2, 3, 4)
    println("multiString sum -------> " + i)
    for (x <- args) {
      println("multiString -------> " + x)
    }
  }

  /**
    * 函数字面量
    */
  def customFuc(): Unit = {
    //将函数字面量赋值给一个变量
    val add = (a: Int, b: Int) => a + b
    //调用
    add(1, 3)
    // 还可以这样调用，
    val i = add.apply(1, 3)
    println("customFuc -------> " + i)
    //另外一种写法：
    val fun: Int => String = mInt => "The value of mInt is " + mInt.toString
    println(fun(10))

  }

  /**
    * 高阶函数
    */
  def highOrderFunc(): Unit = {
    val nums = Seq(1, 2, 3, 4)
    val doubleNum = (x: Int) => x * 5
    val newNum = nums.map(doubleNum)

    println("highOrderFunc -------> " + newNum)
  }

  /**
    * 函数柯理化Function Currying
    *
    * 函数柯理化就是把接受多个参数的函数变换成接受一个单一参数（最初函数的第一个参数）的函数，
    * 并且返回接受余下的参数而且返回结果的新函数的技术
    */
  def curryFunc: Unit = {
    def add(x: Int, y: Int) = x + y

    //经过柯理化可变化为：
    def add1(x: Int)(y: Int) = x + y

    val value = add1(1)(2)
    val v2 = add1(1) _
    println("curryFunc -------> " + value)
    println("curryFunc -------> " + v2(2))
  }

  /**
    * 集合可分为可变的集合（scala.collection.mutable）和不可变的集合（scala.collection.immutable）
    * 默认情况下，Scala 一直采用不可变集合类。
    */
  def list: Unit = {
    var city: List[String] = List("Beijing", "Shanghai", "Shenzhen", "Chengdu");
    var city2: List[String] = List.apply("Beijing", "Shanghai", "Shenzhen", "Chengdu");
    var city3: List[String] = "Beijing" :: ("Shanghai" :: ("Shenzhen" :: ("Chengdu" :: Nil)))
    var city4: List[String] = "Beijing" :: "Shanghai" :: "Shenzhen" :: "Chengdu" :: Nil
    val city5: List[String] = "zhao" :: ("qian" :: ("sun" :: ("li" :: ("zhou" :: Nil))))

    println("list -------> " + city)
    println("list -------> " + city2)
    println("list -------> " + city3)
    println("list -------> " + city4)
    println("list -------> " + city5)

    for (i <- city) {
      if (i.equals(city.head)) {
        println("city's position 0 is " + city.apply(0))
      }
    }
    //列表连接
    val citys = city ::: city5
    println("citys are " + citys)
  }

  /**
    * Scala的set是没有重复对象的集合
    */
  def set(): Unit = {
    val s1 = Set(1, 2, 3, 4, 1)
    val s2 = Set("Math", "English", "History")
    println("set -------------> " + s1)
    println(s1 ++ s2)

    val bool = s1.apply(1)
    if (bool) {
      println("set -----s1 contains " + 1)
    }
  }

  /**
    * Map就是键值对类型的集合
    */
  def map(): Unit = {
    val areaCode = Map("010" -> "北京", "021" -> "上海", "0755" -> "深圳", "028" -> "成都")
    for (x <- areaCode) {
      println("map --------> " + x)
    }
    val city = areaCode.apply("021")
    println("Map  apply--------> " + city)
  }

  /**
    * 由多个单值包含在圆括号中构成的集合被成为元组。构成元组的值的类型可以不同，但值不能改变。元组成员的个数最多不能超过22个
    */
  def tuple(): Unit = {
    val t1 = (1, "mike", 4.0f, 3.6d)
    val t2 = new Tuple4(1, "mike", 4.0f, 3.6d)
    val t3 = new Tuple4[Int, String, Float, Double](1, "mike", 4.0f, 3.6d)
    val (length, width, height) = (10, 20, 30)

    println("tuple  --------> " + t1)
    println("tuple  --------> " + t2)
    println("tuple  --------> " + t3)
    println("tuple  --------> " + length + width)

    //元组的访问可以通过下标，迭代器或者名字来访问。

    //元组的下标从1开始。比如要访问第二个元素，可以表示为：tuple._2

    println("tuple  --------> float is " + t1._3 + " double is " + t1._4)
  }

  /**
    * Option(选项)类型用来表示一个值是可选的（有值或无值)。
    * Option[T] 表示一个类型为 T 的可选值的容器。 如果值存在， Option[T] 就是一个 Some[T] ，如果不存在， Option[T] 就是None
    */
  def option(): Unit = {
    val m: Map[Int, String] = Map(100 -> "北", 101 -> "上", 102 -> "广", 104 -> "深")

    val v1: Option[String] = m.get(100)
    val v2: Option[String] = m.get(110)

    println("option  --------> " + v1)
    println("option  --------> " + v2)
  }

  /**
    * 迭代器不是集合，只是访问集合的一种方式。最常用的方法为next()返回下一个元素和hasNext()检查是否还有元素
    */
  def iterator(): Unit = {
    val i = Iterator(1, 2, 3)
    while (i.hasNext) {
      println("iterator --------> " + i.next())
    }
    val seq = Seq(2, 3, 4)
    val it = seq.iterator
    while (it.hasNext) {
      println("iterator seq--------> " + it.next())
    }

    val test = new Test
    test.run(Array(""))
  }

}

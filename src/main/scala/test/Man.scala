package test

import scala.util.control.Breaks
import scala.util.control.Breaks._

class Man(val firstName: String) extends Human {
  override def print(): Unit = println("Men are an important part of humanity")

  override def name(): String = {
    return firstName
  }

  /**
    * 偏函数PartialFunction
    *
    * 如果想定义一个函数，让它在只接受其他参数定义范围的子集，对于超出这个范围的其他值则抛出异常。这就是偏函数。换句话说：偏函数指仅定义
    * 了输入参数的子集的函数。
    *
    * 偏函数有一个重要的方法isDefinedAt，用来判断入参是否属于处理范围。
    *
    * PartialFunction[A,B]，接收一个类型为A的参数，返回一个类型为B的结果。下面是偏函数PartialFunction的类型定义：
    * trait PartialFunction[-A, +B] extends (A => B)
    * 可以看出偏函数是一种特质。这里的”-”表示逆变，通俗的解释，-A表示A的子集。例如，String是AnyRef的子集。
    * 同理，“+”称为协变(covariance)，通俗的讲，+B表示 B的超集。例如： AnyRef是String的超集。
    *
    *
    * 总结：
    * [1] 偏函数是一元函数（unary function）；
    * [2] 偏函数是一个将类型A转换为类型B的特质；
    */

  val isEven: PartialFunction[Int, String] = {
    case x => {
      if (x % 2 == 0) {
        x + " is even"
      } else {
        x + " is odd"
      }
    }
  }

  /**
    * 在Scala中没有break和continue关键字，是通过定义的一个Breaks类来实现的
    */
  def breaks(): Unit = {
    //实现方式1
    val loop = new Breaks

    loop.breakable {
      for (i <- 1 to 10) {
        println(i)
        if (i == 5) {
          loop.break()
        }
      }
    }

    //实现方式2
    breakable {
      for (i <- 0 until 10) {
        println(i)
        if (i == 3) {
          break()
        }
      }
    }
  }

  /**
    * Scala中的continue是通过把需要continue的部分用breakable包起来，然后break来实现的
    */
  def continue(): Unit = {
    for (i <- 0 to 5) {
      breakable {
        if (i % 2 == 0) {
          break
        }
        println("奇数为：" + i)
      }
    }
  }
}

object Man {
  def main(args: Array[String]): Unit = {
    val man = new Man("明犯强汉者，虽远必诛！")
    val name = man.firstName
    println(name)
    man.print()
    println(man.walk())
    val even = man.isEven(4)
    val odd = man.isEven(3)

    println("even is " + even + " odd is " + odd)

    man.breaks()

    man.continue()

    val hj = new HumanJava("Lindy")
    val name1 = hj.getName
    println(name1)
  }


}

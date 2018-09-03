package test

/**
  * 特质用于在类之间共享接口和字段，也称为“特征”。它有点类似于Java8的interface，类和对象都可以继承特质
  *
  * Scala中trait的特点包括：
  * 可以定义抽象方法；
  * 可以定义实现的方法；
  * 可以继承多个trait；
  */
trait Human {
  def name(): String

  def print()

  def walk(): String = "Every one is different from others"
}

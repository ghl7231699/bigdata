package test

/**
  * 类继承
  *
  *
  *
  * Scala编译器为case类重写了toString()，equals()，hashCode()和copy()方法
  * 普通类的toString()方法是返回的引用地址，而case类返回里面的值
  */
class Dog extends Animal {
  var name = "Golden Retriever "

  override def eat(): Unit = println("Dogs are faithful friends of men")
}

case class Cat(name: String,age:Int) {
//  override def toString: String = "My name is " + name
}


object Dog {
  def main(args: Array[String]): Unit = {
    val dog = new Dog()
    dog.eat()
    println(dog.name)
    val cat = Cat("Kitty",2)
    val name = cat.name
    println(name)
    println(cat.toString)
    val cat1 = cat.copy()
    println(cat1)
    println(dog.toString)
    println(cat.equals(cat1))
  }
}

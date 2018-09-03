package test

/**
  * [1] 直接在类名后面跟参数为主构造函数;
  * [2] 可以通过this定义辅助构造函数
  *
  * @param n
  * @param a
  */
class Person(n: String, a: Int) {

  var name: String = n
  var age = a

  def this(a: Int) {
    this("", a)
  }

  override def toString: String = "name: " + name + " age：" + a

}

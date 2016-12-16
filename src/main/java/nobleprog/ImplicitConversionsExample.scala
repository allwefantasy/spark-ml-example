package nobleprog

/**
 * 12/13/16 WilliamZhu(allwefantasy@gmail.com)
 */
object ImplicitConversionsExample {
  def main(args: Array[String]): Unit = {
    import AConversions._
    val a = new A()
    println(a.b)
  }
}

class A {
  def a = {
    "A.a"
  }
}

class B(a: A) {
  def b = {
    "B.b"
  }
}

object AConversions {
  implicit def mapAToB(a: A): B = {
    new B(a)
  }
}

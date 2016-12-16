package nobleprog

/**
 * 12/13/16 WilliamZhu(allwefantasy@gmail.com)
 */

object TraitExample {
  def main(args: Array[String]): Unit = {
    println(new TraitExample().a)
    println(new TraitExample().b)
  }
}

class TraitExample extends ABC {
  override def a: String = {
    "a method override"
  }
}

trait ABC {
  def a: String

  def b = {
    "b method called"
  }

}

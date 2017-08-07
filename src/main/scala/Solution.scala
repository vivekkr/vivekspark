object Solution extends App {

  def solution(a: Array[Int]): Int = {

    var list = List.empty[Int]
    a map {
      case -1 => list
      case x => list = a(x) +: list
    }
    list.length
  }
  solution(Array(1, 4, 5, 3, 2, -1))
}

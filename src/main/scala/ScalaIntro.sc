//We define an immutable String
val hello = "hola"

//We define a mutable String specifying the typing
var world: String = "mundo"

println(hello + world)

world = "a todos"

println(hello + world)
println(s"$hello $world")
s"$hello $world"

//1 )Define una funcion que pasado un entero devuelva el cuadrado de ese enteroo, sin que sea necesario definir el tipo de lo que se va a devolver
def mult1(x: Int) = {
  x * x
}

//2) Define una funcion a la que pasado un entero, devuelva el cuadrado de ese entero, definiendo el tipo de respuesta
def mult2(x: Int): Int = {
  x * x
}

//3) Lo mismo que antes, pero que sino se le pasa un entero siempre devuelva un 1
def multDefaultValue(x: Int = 1) = {
  x * x
}

def sumRecursive(n: Int): Int = {
  if (n == 0) 0 else n + sumRecursive(n - 1)
}

def sumTailRecursive(n: Int): Int = {
  def sum(n: Int, acc: Int): Int = {
    if(n == 0) acc
    else sum(n - 1, acc + n)
  }
  sum(n, 0)
}

val list: List[Int] = List(1, 2, 3)

list.map(elemento => mult2(elemento))

list.map(mult2(_))

val tupla: List[(Int, String)] = List((1,"a"), (2,"b"), (3,"c"))

tupla.map(tupla => (sumTailRecursive(tupla._1), tupla._2.toUpperCase))

tupla.map{case(firstElement, secondElement) => (sumTailRecursive(firstElement), secondElement.toUpperCase())}

tupla.map{case(firstElement, _) => sumTailRecursive(firstElement) }

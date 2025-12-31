package part1recap

import scala.concurrent.Future
import scala.util.{Failure, Success}

object ScalaRecap extends App {

  val aBoolean: Boolean = false

  val anyIfExpression = if(2>3) "bigger" else "smaller"

  // Instructions vs expressions
  val theUnit = println("Hello Scala") // Unit = no meaningful value

  def myFunction(x: Int) = 42

  // OOP
  class Animal
  class Cat extends Animal
  trait Carnivore {
    def eat(animal: Animal): Unit
  }

  class Crocodile extends Animal with Carnivore {
    override def eat(animal: Animal): Unit = println("Crunch!")
  }

  // Singleton pattern
  object MySingleton

  // Companions
  object Carnivore

  // generics
  trait MyList[+A] // + covariant, * contravariant

  // method notation
  val x = 1 + 2
  val y = 1.+(2)

  // Functional Programming
  val incrementor: Function1[Int, Int] = new Function1[Int, Int] {
    override def apply(x: Int): Int = x + 1
  }
  val sameIncrementor: Int => Int = x => x + 1
  val incremented = incrementor(42) // returns 43

  // map, flatMap, filter
  val processedList = List(1,2,3).map(sameIncrementor)

  // Pattern matching
  val unknown: Any = 45
  val ordinal = unknown match {
    case 1 => "first"
    case 2 => "second"
    case _ => "unknown"
  }

  // try-catch
  try {
    throw new NullPointerException
  } catch {
    case _: NullPointerException => "some returned value"
    case _ => "something else"
  }

  // Future
  import scala.concurrent.ExecutionContext.Implicits.global
  val aFuture = Future {
    // some expensive computation, runs on another thread
    42
  }

  aFuture.onComplete {
    case Success(meaningOfLife) => println(s"I've found $meaningOfLife")
    case Failure(exception) => println(s"I have failed $exception")
  }

  // Partial functions
  val aPartialFunction= (x: Int) => x match {
    case 1 => 43
    case 8 => 56
    case _ => 999
  }
  val samePartialFunction: PartialFunction[Int, Int] = {
    case 1 => 43
    case 8 => 56
    case _ => 999
  }

  // Implicits
  // auto injecteion by compiler
  def methodWithImplicitArgument(implicit x: Int) = x + 43
  implicit val implicitInt = 67
  val implicitCal = methodWithImplicitArgument // compiler figures this out

  // implicit conversions - implicit defs
  case class Person(name: String) {
    def greet =  println(s"Hi, my name is $name")
  }
  implicit def fromStringToPerson(name: String) = Person(name)
  "Bob".greet // fromStringToPerson("Bob").greet

  // implicit conversion - implicit classes
  implicit class Dog(name: String) {
  def bark = println("Bark!")
  }
  "Lassie".bark // because of implicit class, string is auto converted into Dog and as a result the bark method is available.

  /* which implicit to use?
   - local scope
   - imported scope
   - companion objects of the types involved in the method call
   */
}

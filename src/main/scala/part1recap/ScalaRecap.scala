package part1recap

import scala.concurrent.Future
import scala.util.{Failure, Success}

object ScalaRecap extends App {
  //values and variables
  val aBoolean: Boolean = false
  // expressions
  val anIfExpression = if (2 > 3) "bigger" else "smaller"

  //instructions vs expressions

  val theUnit = println("Hello, Scala")

  //functions
  def myFunction(x: Int) = 42

  //OOP
  class Animal

  class Cat extends Animal

  trait Carnivore {
    def eat(animal: Animal): Unit
  }

  class Crocodile extends Animal with Carnivore {
    override def eat(animal: Animal): Unit = println("Crunch!!")
  }

  //Singleton Pattern : object xxx, by doing this in one line I've defined both the type my Singleton and the single instance that this type can have.

  object MySingleton

  object Carnivore // Companions

  // generics
  trait MyList[A]

  //method notation
  val x = 1 + 2
  val y = 1.+(2)

  // Functional Programming
  val incrementer: Int => Int = x => x + 1
  val incremented = incrementer(42)

  println(incremented)

  // map, flatMap, filter

  val procssedlist = List(1, 2, 3).map(incrementer)
  procssedlist.foreach(elem => println(elem))

  //Pattern Matching

  val unknown: Any = 45
  val oridinal = unknown match {
    case 1 => "first"
    case 2 => "second"
    case _ => "unknown"
  }
  println(oridinal)


  // Try-catch
  try {

    throw new NullPointerException
  }
  catch {
    case _: NullPointerException => "some returned value"
    case _ => "something else"
  }


  // Future

  import scala.concurrent.ExecutionContext.Implicits.global

  val aFuture = Future {
    // some expensive computation , runs on another thread
    42
  }
  aFuture.onComplete {
    case Success(meaningOfLife) => println(s"I've found $meaningOfLife")
    case Failure(ex) => println(s"I have failed :$ex")
  }

  //Partial functions
  val aPartialFunction = (x: Int) => x match {
    case 1 => 48
    case 8 => 56
    case _ => 999
  }

  //Implicits
  //auto injection by the compiler

  def methodWithImplicitArgument(implicit x: Int) = x + 43

  implicit val implicitInt = 67
  val implicitCall = methodWithImplicitArgument
  println(implicitCall)

  // Implicit conversions - implicit defs

  case class Person(name: String) {
    def greet = s"Hello, my name is $name"
  }

  implicit def fromStringToPerson(name: String) = Person(name)

  "Bob".greet // fromStringToPerson("Bob").greet

  // implicit conversion -implicit classes

  implicit class Dog(name: String) {
    def bark = println("Bark!!")
  }

  "Lassie".bark //The compiler will automatically convert this String into a dog with that content as an argument

  // How the compiler figures out which implicit to inject here into the argument list ??
  // It is done  by looking into three areas.

  /*
  1- Local scope  as  implicit val implicitInt = 67 : Explicitly defining an implicit value here in the same scope

  2- Imported Scope as  import scala.concurrent.ExecutionContext.Implicits.global

  3- Companion objects of the type involved in the method call
   */

}

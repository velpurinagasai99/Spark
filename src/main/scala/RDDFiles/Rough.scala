package RDDFiles
/*
sc.defaultParallelism()
rdd.getNumPartitions()
sc.defaultMinPartitions()

val rdd2 = rdd1.repartition(8)

when filter is applied the data decreases/increases then we use repartition(wideTransformation)

coalesce(only decreases partitions) does internal merging and do shuffling whereas repartition directly do
shuffling. Hence coalesce is faster.

Check difference between Transformations(Wide and Narrow) and Actions
 In scala object is different from instance of a class
* */
object Rough {
  object people {
    val N_ears = 2
    def canfly: Boolean = false
  } //this is same as static, only one object cannot have instance(i.e, singleton in Java)
  class people {
    //Here we can give instance level variables.
  }
  class person(name: String, age: Int)
  {
    val n=name
    def pr() {println(name)}
  }
  val p= new person("Sai",20)
  p.pr()
  println(p.n)


  class Animal {
   protected def eat = println("Animals Eat a lot")
  }
  class Cat extends Animal{
    eat                                   //Protected methods can be called in child class but not in outside of child class
    def sleep() = println("Cat sleeps a lot")
  }
  val cat = new Cat()
  //cat.eat()                           //Protected methods can not be called outside Child class
  cat.sleep()



//  Important
//  ********************************************Trait and Abstract class both are similar
//  trait is used for multiple inheritence which is not possible with abstract.(Similar to interfaces in Java, but Interfaces are completely un-implemented, where
//    as traits can be partially implemented. Traits are behaviour)
//  Once trait is inherited all the un-implemented methods should be implemented in child class
  abstract class animal {
    def animalType
  }

  trait carnivore{
    def mealPreference
  }

  trait omnivore{
    def mealPreference2
  }

  class Crocodile extends animal with carnivore with omnivore {
    val animalType = println("Animal")
    val mealPreference = println("Carnivore meal Type")
    val mealPreference2 = println("omnivore meal Type")

  }


//  *******************************************CASE CLASS************************************************************************************
//
//  Class parameters are promoted to fields, which means we can access outside of the class too.
//  Companion(Static class object) is already created.
//    Have handy copy method.
//  Case classes are serializable

  object caseClassStudy{
    case class person(name: String, age: Int)

    val p= new person("Sai",26)

    println(p.name)				//If it is normal class then we would get error
    println(p,p.toString)		//This results in person("Sai",26) for both which is different in normal class

    val p2=  new person("Sai",26)

    if(p==p2)
      println("Equal")			//Instead of checking references it compares values

    val p3=person.apply("Sumit",30)	//This acts as companion class
    val p4=person.apply("Suda",28)
    val p5=p4.copy()
  }

  def pp(name: String*) ={
    for (i<-name){
      println(i)
    }
  }
  pp("Hello","How","are you")

//  *****************************************Difference between NIL, NULL, NONE, NOTHING, UNIT, OPTION****************************************************************
//
//  NULL is a traint in scala, where one instance is exists(null)
//  Unit is a void return type
//  NIL is a empty list
//    Nothing is a trait which has no instances. Nothing is return type of exception or error for a defined function.
//    Option-While writing a function sometimes we have something to return sometimes we dont...In this case we use Option(It avoids Null pointer exception)

  def mayReturnString(num: Int): Option[String]={
    if(num>0) Some("A positive number")
    else None
  }
  def check(num: Int)={
    mayReturnString(num) match{
      case Some(str) => println(str)
      case None => println("No String")
    }
  }
  check(1)
  check(-10)

//  *******************************************************YEILD********************************************************************************************************
  val b = for(i <- 1 to 10) yield {
    i*i}
  print(b)					//Now it is vector with out yeild it is unit

//  Diamond Problem is a problem with multiple inheritence
//    Monad is an object that wraps another object.
//  Week-9 Scala Interview preparation video 5
//  Design Patterns:
//    1. Factory Design pattern-> Separate instance creation logic seperate from cliient visibility.
//
//    Flatmap accepts one parameter, Flatmapvalues accepts two values of two tuples.


  //***************************************Week10- Shared Variables********************************************************
  // Broad cast variable - is same as map side Join in HIve(Separate copy on each machine0
  //Accumlator - a counter (a single copy on driver machine)


// Yarn architecture video: https://youtu.be/KqaPMCMHH4g?si=qBtkUhzW_gYkPs3F
// Some Transformations and actions: https://spark.apache.org/docs/latest/rdd-programming-guide.html#transformations
//.toDebugString
//  Map Partition is executed on Partition level
}

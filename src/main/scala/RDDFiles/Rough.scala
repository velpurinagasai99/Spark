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
}

package Practice
/*
sc.defaultParallelism()
rdd.getNumPartitions()
sc.defaultMinPartitions()

val rdd2 = rdd1.repartition(8)

when filter is applied the data decreases/increases then we use repartition(wideTransformation)

coalesce(only decreases partitions) does internal merging and do shuffling whereas repartition directly do
shuffling. Hence coalesce is faster.

Check difference between Transformations(Wide and Narrow) and Actions

* */
object Rough {

}

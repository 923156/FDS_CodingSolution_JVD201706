
#Top ten most viewed movies with their movies Name (Ascending or Descending order)

object top10_movies {
 def main(args: Array[String]): Unit = {
   val data=sc.textFile("file:///home/acadgild/Downloads/ml-1m/ratings.dat")
   val processed=data.map(x=> (x.split("::")(1),1)).reduceByKey((x,y)=> x+y).map(x=>(x._2,x._1)).top(10).map(x=> (x._2,x._1))
   processed.foreach(println)
   val movname=sc.textFile("file:///home/acadgild/Downloads/ml-1m/movies.dat")
   val mov_names=movname.map(x=>  (x.split("::")(0),x.split("::")(1)))
   val output=mov_names.join(sc.parallelize(processed)).map(x=> (x._2._2,x._2._1)).sortByKey(ascending=false, 1)
   output.foreach(println)
  }
}

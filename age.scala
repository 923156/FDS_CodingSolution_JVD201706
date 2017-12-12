#We wish to know how have the genres ranked by Average Rating, for each profession and age group. The age groups to be considered are: 18-35, 36-50 and 50+.

object table {
   def main(args: Array[String]): Unit = {
    val data=sc.textFile("file:///home/acadgild/Downloads/ml-1m/ratings.dat")
    val processed=data.map(x=> (x.split("::")(1),(x.split("::")(0),x.split("::")(2).toInt,1)))
    val user_data=sc.textFile("file:///home/acadgild/Downloads/ml-1m/users.dat")
    val processed_user=user_data.map(x=> (x.split("::")(0),(x.split("::")(2).toInt,x.split("::")(3)))).filter(x=> x._2._1>1).map(x=>map_age(x))
    val movies_list=sc.textFile("file:///home/acadgild/Downloads/ml-1m/movies.dat")
    val movie_names=movies_list.map(x=>  (x.split("::")(0),x.split("::")(1),x.split("::")(2))).flatMap(x=> flat_genre(x))
    val moviename_rating_user=processed.join(movie_names).map(x=>(x._2._1._1,(x._2._1._2,x._2._1._3,x._2._2._1,x._2._2._2))).join(processed_user).map(x=> ((x._2._2._1,x._2._2._2,x._2._1._4),(x._2._1._1,x._2._1._2))).reduceByKey((x,y)=>(x._1+y._1,x._2+y._2)).map(x=>(x._1,x._2._1.toDouble/x._2._2)).map(x=> ((x._1._1,x._1._2),List((x._2,x._1._3)))).reduceByKey((x,y)=> x:::y).map(x=> (x._1,x._2.sorted(Ordering[(Double,String)].reverse))).map(x=> (x._1,x._2.map(x=> x._2)))
   moviename_rating_user.foreach(println)

}
def flat_genre(x:(String,String,String))={
     val genres=x._3.split("\\|")
     var mylist=List[(String,(String,String))]()
     for(t <- genres)
     {
       mylist=(x._1,(x._2,t))::mylist
     }
      mylist
   }
   def map_age(x:(String,(Int,String)))={
     var age=""
     if(x._2._1==18 || x._2._1==25)
     {
       age="18-35"
     }
     else if( x._2._1==35 || x._2._1==45)
     {
       age="36-50"
     }
     else
     {
       age="50+"
     }
     (x._1,(age,x._2._2))
   }

}


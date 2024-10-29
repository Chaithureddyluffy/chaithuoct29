import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
object Scala {
    def main(args:Array[String]): Unit = {
      /*val spark=SparkSession.builder()
        .appName("spark-program")
        .master("local[*]")
        .getOrCreate()
      val df=spark.read
        .format("csv")
        .option("header",true)
        .option("path","C:/Users/poola/Desktop/DOC/chaithu.csv")
        .load()
      df.show()*/
      val spark=SparkSession.builder()
        .appName("spark-program")
        .master("local[*]")
        .getOrCreate()
      val df=spark.read
        .format("csv")
        .option("header",true)
        .option("inferschema",true)
        .option("path","C:/Users/poola/Desktop/DOC/details.csv")
        .load()

      df.printSchema()
      df.show()
      //WORDCOUNTPROGRAMME//
      /*val input=sc.textFile("C:/Users/poola/Desktop/chaithuIP.txt")
      val rdd1=input.flatMap(x=>x.split(" "))
      val rdd2=rdd1.map(x=>(x,1))
      val rdd3=rdd2.reduceByKey((x,y)=>x+y)
      val rdd4=rdd3.sortBy(x=>x._2)
      rdd4.collect().foreach(println)*/
      /*val arr=Array(1,2,3,4,5)
      val rdd1=sc.parallelize(arr)
      val rdd2=rdd1.mean()
      print(rdd2)*/
      //FINDIND THE MEAN WITHOUTFUNCTION//
      /*val arr=Array(10,20,30,40,50,60,70,80,91)
      val rdd1=sc.parallelize(arr)
      val rdd2=rdd1.sum()
      val rdd3=rdd1.count()
      val rdd4=rdd2/rdd3
      println(rdd4)*/
      //scala.io.StdIn.readLine()
      //METHOD2 FOR FINDING THE MEANS WITHOUTFUNCTION
      /*val arr=Array(10,20,30,40,50,60,70,80,91)
      val rdd1=sc.parallelize(arr)
      val sum=rdd1.reduce((x,y)=>(x+y))
      val c=rdd1.count()
      val z=sum/c.toDouble
      print(z)*/
      //OUTPUTSAVING//
      /*val arr=Array(1,2,3,5,6,6,7)
      val rdd1=sc.parallelize(arr)
      rdd1.saveAsTextFile("C:/Users/poola/Desktop/DOC/opt")*/
      //INNERJOIN//
      /*val arr=Array((1,"chaithu"),(2,"hydra"))
      val arr2=Array((1,"hdgdgd"),(4,"hdgdh"))
      val rdd1=sc.parallelize(arr)
      val rdd2=sc.parallelize(arr2)
      val rdd3=rdd1.leftOuterJoin(rdd2)
      rdd3.collect().foreach(println)*/
      /*val arr=Array(1,2,3,4,5)
      val arr1=Array(3,4,5,6)
      val rdd1=sc.parallelize(arr)
      val rdd2=sc.parallelize(arr1)
      val rdd3=rdd1.filter(x=>x%2==0)
      //val rdd4=rdd3.map(x=>(x,1))
      //val rdd5=rdd4.reduceByKey((x,y)=>x+y)
      //val rdd5=rdd4.sortBy(x=>x,true)
      rdd3.collect().foreach(println)*/
      //val arr=Array("apple","chaihu","hdgd")
      //val arr=Array(1,2,3,4,5)
      //val rdd1=sc.parallelize(arr)
      //val serch="app"
      //val rdd2=rdd1.filter(x=>x==serch)
      //val rdd2=rdd1.filter(x=>x.contains(serch))
      //rdd2.collect().foreach(println)
      /*val arr=Array("chaithanya")//,"chaithu","hydrra","chaithu")
      val rdd1=sc.parallelize(arr)
      val rdd2 =rdd1.flatMap(x=>x.split(""))
      val rdd3=rdd2.map(x=>(x,1))
      val rdd4=rdd3.reduceByKey((x,y)=>x+y)
      val rdd5=rdd4.sortBy(x=>x._2,false)
      rdd5.take(10).foreach(println)*/
      /*val rdd1=Array(1,2,3,4,5,6)
      val rdd2=sc.parallelize(rdd1)
      val rdd3=rdd2.filter(x=>x%2==0)
      val rdd4=rdd3.count()
      //println(s"Even numbers: ${rdd3.collect().mkString(", ")}; Count of even numbers: $rdd4")
      println(s"Even Numbers:${rdd3.collect().mkString(",")};Count${rdd4}")*/
      /*val arr=Array(1,2,3,4,5)
      val rdd1=sc.parallelize(arr)
      //val rdd2=rdd1.map(x=>(x,1))
      //val rdd3=rdd2.reduceByKey((x,y)=>x+y)
      //val rdd4=rdd3.fold(0)((x,y)=>x+y)
      val rdd2=rdd1.fold(0)((x,y)=>x+y)
      print(rdd2)*/
      // val arr=Array(("hello", 2), ("world", 3), ("hi", 1), ("foo", 4), ("bar", 2))
      /*val arr=Array("hello", "world", "hello",
        "world", "world", "hello", "hi")
      val rdd1=sc.parallelize(arr)
      val rdd2=rdd1.distinct()
      rdd2.collect().foreach(println)*/


      /*val rdd1=sc.parallelize(arr)
      val rdd2=rdd1.flatMap(x=>x.split(" "))
      val rdd3=rdd2.map(x=>(x,1))
      val rdd4=rdd3.reduceByKey((x,y)=>x+y)
      val rdd5=rdd4.sortBy(x=>x._2,false)
      rdd5.collect().foreach(println)*/
      /*val arr=Array(1,2,3,4,5)
      val rdd1=sc.parallelize(arr)
      val rdd2=rdd1.reduce((x,y)=>x+y)
      print(rdd2)*/
      /*val rdd1 = sc.parallelize(Array((1, "apple"),
        (2, "banana"), (3, "orange")))
      val rdd2 = sc.parallelize(Array((1, "red"),
        (2, "yellow"), (4, "green")))
      val joinedRdd = rdd1.rightOuterJoin(rdd2)
      joinedRdd.foreach(println)*/



    }

}

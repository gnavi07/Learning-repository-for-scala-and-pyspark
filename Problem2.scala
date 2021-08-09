import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._



object Problem2 extends App{
  
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.Name","my app")
  sparkConf.set("spark.master","local[2]")
  
  val spark = SparkSession.builder()
  .config(sparkConf)
  .getOrCreate()
  
  val ratingsRdd = spark.sparkContext.textFile("/Users/smili/Documents/Big data -Naveena/Week 12/dataset for assignment/ratings.dat")
    
    val ratingMap = ratingsRdd.map(x => {
      val fields = x.split("::")
      (fields(0),fields(1),fields(2),fields(3))
    })
    
    import spark.implicits._
    
    val ratingsDf=spark.createDataFrame(ratingMap)
    .toDF("movieid","custid","rating","phoneno")
    
    
      val moviesRdd = spark.sparkContext.textFile("/Users/smili/Documents/Big data -Naveena/Week 12/dataset for assignment/movies.dat")
    
    val moviesMap = moviesRdd.map(x => {
      val fields = x.split("::")
      (fields(0),fields(1))
    })
    
    import spark.implicits._
    
    val moviesDf=spark.createDataFrame(moviesMap)
    .toDF("movieid","moviename")
    
    val joinCond = moviesDf.col("movieid") === ratingsDf.col("movieid")
    val joinType = "left"

    val joinedDf=moviesDf.join(broadcast(ratingsDf),joinCond,joinType)
    .drop(ratingsDf.col("movieid"))
    
    joinedDf.createOrReplaceTempView("Movies")
    val sumRatDf=spark.sql("select movieid,moviename,sum(rating)/count(*) as Avg from movies group by movieid,moviename order by Avg desc")
    .show(false)

spark.stop()
}
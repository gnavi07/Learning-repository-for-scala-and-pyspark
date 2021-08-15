import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._



object topMovies extends App{
  
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.Name","Movie Rating")
  sparkConf.set("spark.master","local[2]")
  
  val spark = SparkSession.builder()
  .config(sparkConf)
  .getOrCreate()
  
  val ratingsRdd = spark.sparkContext.textFile("<input file path>/ratings.dat")
    
  val ratingMap = ratingsRdd.map(x => {
  val fields = x.split("::")
      (fields(0),fields(1),fields(2),fields(3))
    })
    
    import spark.implicits._
    
    val ratingsDf=spark.createDataFrame(ratingMap)
    .toDF("movieid","custid","rating","phoneno")
    
    
    val moviesRdd = spark.sparkContext.textFile("<input file path>/movies.dat")
    
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

/*Result:
+-------+------------------------------------------+------------------+
|movieid|moviename                                 |Avg               |
+-------+------------------------------------------+------------------+
|283    |New Jersey Drive (1995)                   |4.962962962962963 |
|2339   |I'll Be Home For Christmas (1998)         |4.956521739130435 |
|3324   |Drowning Mona (2000)                      |4.904761904761905 |
|3902   |Goya in Bordeaux (Goya en Bodeos) (1999)  |4.890909090909091 |
|446    |Farewell My Concubine (1993)              |4.8431372549019605|
|447    |Favor, The (1994)                         |4.837837837837838 |
|1131   |Jean de Florette (1986)                   |4.796116504854369 |
|682    |Tigrero: A Film That Was Never Made (1994)|4.733333333333333 |
|1670   |Welcome To Sarajevo (1997)                |4.714285714285714 |
|3461   |Lord of the Flies (1963)                  |4.702702702702703 |
|1021   |Angels in the Outfield (1994)             |4.694656488549619 |
|1856   |Kurt & Courtney (1998)                    |4.6940298507462686|
|2155   |Slums of Beverly Hills, The (1998)        |4.690140845070423 |
|953    |It's a Wonderful Life (1946)              |4.6878306878306875|
|3596   |Screwed (2000)                            |4.6875            |
|2243   |Broadcast News (1987)                     |4.683333333333334 |
|989    |Schlafes Bruder (Brother of Sleep) (1995) |4.681818181818182 |
|101    |Bottle Rocket (1996)                      |4.679245283018868 |
|1940   |Gentleman's Agreement (1947)              |4.671875          |
|3306   |Circus, The (1928)                        |4.666666666666667 |
+-------+------------------------------------------+------------------+
only showing top 20 rows */

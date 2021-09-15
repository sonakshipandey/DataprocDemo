/* SimpleApp.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{SQLContext, SparkSession}



object GCS-BQ-connector-demo {
  def main(args: Array[String]) {


  	println("\n\n In main ")
    val inputPath = "<replace with gsc path to test file>"
    val outputPath = "<replace with gsc path to output directory>/"
    
    val conf = new SparkConf()
	  val sc = new SparkContext(conf.setAppName("Word Count"))
    val spark = SparkSession
      .builder()
      .appName("SimpleApp")
      .config(conf)
      .getOrCreate()

    spark.conf.set("temporaryGcsBucket", "<bucket-name>")
    
    println("\n\n Fetching input file using GCS connector")
    val lines = sc.textFile(inputPath)
    
    println("\n\n Counting words ")
    val words = lines.flatMap(line => line.split(" "))
    val wordCounts = words.map(word => (word, 1)).reduceByKey(_ + _).repartition(1)
	  
    println("\n\n Printing the word count RDD")
    wordCounts.collect().foreach(println)
    
    println("\n\n Saving file  to GCS")
    wordCounts.saveAsTextFile(outputPath)
    println("\n\n Done ")
	  println("\n\n Creating Dataframe ")
    wordCounts.cache()
    val dfWithSchema = spark.createDataFrame(wordCounts).toDF("word", "count")
    println("\n\n Printing Dataframe ")
    dfWithSchema.show()
    
    println("\n\n Saving Dataframe to a BigQuery Table")
    dfWithSchema
	    .write
	    .mode("overwrite")
	    .format("bigquery")
	    .option("table","<BQ-dataset-name.BQ-table-name>")
	    .save()    
  }
}

package home.spark.spark_first

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object WordCount {
  
  def main(args : Array[String]){
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sparkContext = new SparkContext(conf)
    
    val test = sparkContext.textFile("food.txt");
    
    test.flatMap { line => line.split(" ")}
    
    .map{ word => (word,1)
      
    }
    
    .reduceByKey(_ + _)
    .saveAsTextFile("food.count.txt")
  }
}
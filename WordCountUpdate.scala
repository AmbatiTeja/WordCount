import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext


object WordCountUpdate extends App{
    
  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]","wordcount");
  
  val input = sc.textFile("C:/Users/Akhil/Desktop/Datasets/search_data.txt");
  
  val words = input.flatMap(x => x.split(" "));
  
  val wordsLower = words.map(x => x.toLowerCase())
  
  val  wordMap= wordsLower.map(x => (x,1));
  
  val finalRDD = wordMap.reduceByKey((x,y) => x+y);
  
  val reversedTuple = finalRDD.map(x => (x._2, x._1)).sortByKey();
  
  val sortedResults =  reversedTuple.sortByKey(false).map(x => (x._2,x._1));
  
  val results = sortedResults.collect;
  
  // sortedResults.collect.foreach(println);
  
  scala.io.StdIn.readLine();
  
}
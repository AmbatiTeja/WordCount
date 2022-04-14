import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;


object WordCount extends App{
    
  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]","wordcount");
  
  val input = sc.textFile("C:/Users/Akhil/Desktop/Datasets/search_data.txt");
  
  val words = input.flatMap(x => x.split(" "));
  
  val wordsLower = words.map(x => x.toLowerCase())
  
  val  wordMap= wordsLower.map(x => (x,1));
  
  val finalRDD = wordMap.reduceByKey((x,y) => x+y);
    
  val sortedResults =  finalRDD.sortBy(x => x._2);
  
  val results = sortedResults.collect;
  
  // sortedResults.collect.foreach(println);
  
  for (i <- results){
    val word = i._1
    val count = i._2
    println(s"$word : $count");
  }
  
  scala.io.StdIn.readLine();
  
}
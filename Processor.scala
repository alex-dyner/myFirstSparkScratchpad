package codequiz

import org.apache.spark.rdd.RDD;

object Processor {
  def process(items: RDD[String]): RDD[String] = {
    val filtredItems = items.filter(l => l.startsWith("2015"))
    val itemInfo = filtredItems.map(i => i.split(",")).map(a => ( (a(1), a(2), a(3) ), a(1).toInt + a(2).toInt / a(3).toInt ));
    val result = itemInfo.reduceByKey(_ + _).map(oi => oi._1 + "," + oi._2);
    return result;
  }
}




/* SimpleApp.scala */
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.linalg.Vector

import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import org.apache.spark.mllib.feature.IDF


object SimpleApp {
  def main(args: Array[String]) {
    val logFile = "hdfs://10.10.0.114/tmp/all.txt" // Each document in one line
    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)

    val documents: RDD[Seq[String]] = sc.textFile(logFile).map(_.split(" ").toSeq)
    val hashingTF = new HashingTF()
    val tf: RDD[Vector] = hashingTF.transform(documents)
    tf.cache()
    val idf = new IDF().fit(tf)
    val tfidf: RDD[Vector] = idf.transform(tf)
    val tfidf_results: Int = tfidf.map(v => v.size).reduce((a,b) => a + b)

    println("%s".format(tfidf.first()))
    println("total %s".format(tfidf.first()))
    println("vector length %s".format(tfidf.first().size))
    println("%s".format(tfidf_results))
  }
}


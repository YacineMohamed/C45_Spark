package Initialisation

import org.apache.spark.{SparkConf, SparkContext}

object Spark {
  val conf = new SparkConf()
    .setAppName("C4.5")
    .setMaster("local[*]")

  val sc = new SparkContext(conf)
}
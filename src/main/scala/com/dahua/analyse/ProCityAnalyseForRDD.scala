package com.dahua.analyse

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object ProCityAnalyseForRDD {
  def main(args: Array[String]): Unit = {
    var conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val spark = SparkSession.builder().config(conf).appName("Log2Parquet").master("local[1]").getOrCreate()

    val sc: SparkContext = spark.sparkContext

    var Array(inputPath, outputPath) = args

    val line: RDD[String] = sc.textFile(inputPath)
    val field: RDD[Array[String]] = line.map(_.split(",", -1))
    val proCityRDD: RDD[((String, String), Int)] = field.filter(_.length >= 85).map(arr => {
      var pro = arr(24)
      var city = arr(25)
      ((pro, city), 1)
    })
    // 降维。
    val reduceRDD: RDD[((String, String), Int)] = proCityRDD.reduceByKey(_ + _)

    val rdd2: RDD[(String, (String, Int))] = reduceRDD.map(arr => {
      (arr._1._1, (arr._1._2, arr._2))
    })

    val num: Long = rdd2.map(x => {
      (x._1, 1)
    }).reduceByKey(_ + _).count()

    rdd2.partitionBy(new HashPartitioner(num.toInt)).saveAsTextFile(outputPath)

    spark.stop()
    sc.stop()
  }
}

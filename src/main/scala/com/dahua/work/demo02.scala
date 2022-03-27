package com.dahua.work

import com.dahua.bean.LogBean
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object demo02 {
  def main(args: Array[String]): Unit = {
    //判断参数是否正确
    if (args.length!=2){

      println(

        """
          |缺少参数
          |inputpath  outputpath
          |""".stripMargin
      )
      sys.exit()
    }
    // 创建sparksession对象
    var conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    // 将自定义对象进行kryo序列化
    conf.registerKryoClasses(Array(classOf[LogBean]))

    val spark = SparkSession.builder().config(conf).appName("Log2Parquet2").master("local[*]").getOrCreate()

    var sc = spark.sparkContext

    import spark.implicits._

    //接收参数
    var Array(inputPath,outputPath)=args
    var rdd:RDD[Array[String]]=sc.textFile(inputPath).map(_.split(",",-1)).filter(_.length>=85)
    val proCityRDD: RDD[((String, String), Int)] = rdd.filter(_.length >= 85).map(arr => {
      var pro = arr(24)
      var city = arr(25)
      ((pro, city), 1)
    })
    val reduceRDD: RDD[((String, String), Int)] = proCityRDD.reduceByKey(_ + _)
    reduceRDD.sortBy(-_._2).saveAsTextFile(outputPath)

    spark.stop()
    sc.stop()
  }

}

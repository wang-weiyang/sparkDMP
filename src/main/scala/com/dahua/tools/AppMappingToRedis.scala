package com.dahua.tools

import com.dahua.utils.RedisUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import redis.clients.jedis.Jedis

object AppMappingToRedis {
  def main(args: Array[String]): Unit = {
    // 判断参数是否正确。
    if (args.length != 1) {
      println(
        """
          |缺少参数
          |inputpath  outputpath
          |""".stripMargin)
      sys.exit()
    }



    // RDD 序列化到磁盘 worker与worker之间的数据传输
    var conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    // 创建sparksession对象
    val spark = SparkSession.builder().config(conf).appName("Log2Parquet").master("local[1]").getOrCreate()

    var sc = spark.sparkContext

    import spark.implicits._



    var Array(inputPath)=args
    sc.textFile(inputPath).map(line=>{
      val redis:Array[String] = line.split("[:]",-1)
      (redis(0),redis(1))
    }).foreachPartition(ite=>{
      val jedis:Jedis = RedisUtil.getJedis
      ite.foreach(mapping=>{
        jedis.set(mapping._1,mapping._2)
      })
      jedis.close()
    })
    sc.stop()
  }
}

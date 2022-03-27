package com.dahua.tools

import com.dahua.bean.LogBean
import com.dahua.utils.RedisUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis

object AppMappingToRedisRead {
  def main(args: Array[String]): Unit = {
    /**
     * 从Redis 读取进行空值的替换
     */
    if (args.length != 1) {
      println("缺少参数")
      sys.exit(0)
    }
    var Array(inputPath) = args
    val sparkConf = new SparkConf().setMaster("local[1]").setAppName("DmpLogEtlParquet")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc: SparkContext = spark.sparkContext
    import spark.implicits._
    val log: RDD[String] = sc.textFile(inputPath)
    val logRDD: RDD[LogBean] = log.map(_.split(",", -1)).filter(_.length >= 85).map(LogBean(_)).filter(t => {
      !t.appid.isEmpty
    })
    val rdd1: RDD[LogBean] = logRDD.mapPartitions(log => {

      val jedis: Jedis = RedisUtil.getJedis

      var res = List[LogBean]()
      while (log.hasNext) {
        val bean: LogBean = log.next()
        if (bean.appname == "" || bean.appname.isEmpty) {
          val str = jedis.get(bean.appid)
          bean.appname = str
        }
        res.::=(bean)
      }
      jedis.close()
      res.iterator
    })

    rdd1.map(x=>{

    })

    rdd1.saveAsTextFile("D:\\大数据\\spark\\互联网广告\\互联网广告第一天\\redisOutput")
  }
}

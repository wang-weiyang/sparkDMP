package com.dahua.analyse

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object ProCityAnalyse {

  def main(args: Array[String]): Unit = {
    // 判断参数是否正确。
    if (args.length != 2) {
      println(
        """
          |缺少参数
          |inputpath  outputpath
          |""".stripMargin)
      sys.exit()
    }

    // 创建sparksession对象
    var conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val spark = SparkSession.builder().config(conf).appName("Log2Parquet").master("local[1]").getOrCreate()

    var sc = spark.sparkContext

    import spark.implicits._

    // 接收参数
    var Array(inputPath, outputPath) = args

    val df: DataFrame = spark.read.parquet(inputPath)

    df.createTempView("log")

    // 编写sql语句。
    var sql = "select provincename,cityname,count(*) as pccount from log group by provincename,cityname"

    val resDF: DataFrame = spark.sql(sql)

    val configuration: Configuration = sc.hadoopConfiguration
    // 文件系统对象。
    val fs: FileSystem = FileSystem.get(configuration)

    var path = new Path(outputPath)
    if(fs.exists(path)){
      fs.delete(path,true)
    }
    resDF.coalesce(1).write.partitionBy("provincename","cityname")json(outputPath)

    spark.stop()
    sc.stop()







  }

}

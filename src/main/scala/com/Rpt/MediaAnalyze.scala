
package com.Rpt

import com.utils.RptUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object MediaAnalyze {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("MediaAnalyse").setMaster("local[2]").set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val dic = sc.textFile("/Users/H/Documents/IDEA_Projects/GP_22_DMP_tea/dir/app_dict copy.txt")
    val dicInfo = dic.map(_.split("\t")).filter(_.length>=5).map(x =>(x(4),x(1)) )
    val Broadcast = sc.broadcast(dicInfo.collect().toMap)

    val spark = SparkSession.builder().config(conf).getOrCreate()
    val logs = spark.read.parquet("/Users/H/Documents/IDEA_Projects/GP_22_DMP_tea/out/part-r-00000-9eac08dc-694a-4654-9ad1-777fc288754d.snappy.parquet")
    import spark.implicits._
    val log: Dataset[(String, List[Double])] = logs.map(row => {

      val requestmode = row.getAs[Int]("requestmode")
      val processnode = row.getAs[Int]("processnode")
      val iseffective = row.getAs[Int]("iseffective")
      val isbilling = row.getAs[Int]("isbilling")
      val isbid = row.getAs[Int]("isbid")
      val iswin = row.getAs[Int]("iswin")
      val adorderid = row.getAs[Int]("adorderid")
      val WinPrice = row.getAs[Double]("winprice")
      val adpayment = row.getAs[Double]("adpayment")
      // key 值
      val appid = row.getAs[String]("appid")
      val appname = row.getAs[String]("appname")
      // 创建三个对应的方法处理九个指标
      val req = RptUtils.request2(requestmode: Int, processnode: Int)
      val cli = RptUtils.click(requestmode: Int, iseffective: Int)
      val ad: List[Double] = RptUtils.Ad(iseffective: Int, isbilling: Int, isbid: Int, iswin: Int,
        adorderid: Int, WinPrice: Double, adpayment: Double)
      var appname2 = "11"
      if(appname.equals("未知")||appname.equals("其他")){
        appname2 = Broadcast.value.getOrElse(appid,"XXX")
      }else {
        appname2=appname
      }

      (appname2, req ++ cli ++ ad)
    })
    log.show()
    val res3: RDD[(String, List[Double])] = log.rdd.reduceByKey((x, y) => {
      x.zip(y).map(t => t._1 + t._2)
    })
    res3.map(x =>{
      x._1+","+x._2.mkString(",")
    }).toDF().show()

  }
}

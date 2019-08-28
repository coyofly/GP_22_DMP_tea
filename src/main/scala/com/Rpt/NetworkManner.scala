package com.Rpt

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object NetworkManner {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetWorkManner")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val logs = spark.read.parquet("/Users/H/Documents/IDEA_Projects/GP_22_DMP_tea/out/part-r-00000-9eac08dc-694a-4654-9ad1-777fc288754d.snappy.parquet")
    logs.createOrReplaceTempView("log")

    spark.sql("select networkmannername," +
      "sum(case when REQUESTMODE >= 1 and REQUESTMODE <= 3 then 1 else 0 end) `原始请求数`," +
      "sum(case when REQUESTMODE = 1 and PROCESSNODE >= 2 then 1 else 0 end) `有效请求数`," +
      "sum(case when REQUESTMODE = 1 and PROCESSNODE = 3 then 1 else 0 end) `广告请求数`," +
      "sum(case when ISEFFECTIVE = 1 and ISBILLING = 1 and ISBID = 1 then 1 else 0 end) `参与竞价数`," +
      "sum(case when ISEFFECTIVE = 1 and ISBILLING = 1 and ISWIN = 1 and ADORDERID != 0 then 1 else 0 end) `竞价成功数`," +
      "sum(case when REQUESTMODE = 2 and ISEFFECTIVE = 1 then 1 else 0 end) `展示数`," +
      "sum(case when REQUESTMODE = 3 and ISEFFECTIVE = 1 then 1 else 0 end) `点击数`," +
      "sum(case when ISEFFECTIVE = 1 and ISBILLING = 1 and ISWIN = 1 then 1 else 0 end) `DSP广告消费`," +
      "sum(case when ISEFFECTIVE = 1 and ISBILLING = 1 and ISWIN = 1 then 1 else 0 end) `DSP广告成本`" +
      " from " +
      "log group by networkmannername").show()

    spark.stop()

  }
}

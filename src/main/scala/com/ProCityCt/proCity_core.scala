package com.ProCityCt

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object proCity_core {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("procity").setMaster("local[2]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val frame = spark.read.parquet("/Users/H/Documents/IDEA_Projects/GP_22_DMP_tea/out/part-r-00000-9eac08dc-694a-4654-9ad1-777fc288754d.snappy.parquet")
  import spark.implicits._
    val res1: Dataset[((String, String), Int)] = frame.map(x => {
      val provincename = x.getAs[String]("provincename")
      val cityname = x.getAs[String]("cityname")
      ((provincename, cityname), 1)
    })
    val res2: RDD[((String, String), Int)] = res1.rdd.reduceByKey(_+_)
    val tup = res2.map(x => {
      val provincename: String = x._1._1
      val cityname: String = x._1._2
      val ct = x._2
      (provincename, cityname,ct)
    })
    val load = ConfigFactory.load()
    val prop = new Properties()
    prop.setProperty("user",load.getString("jdbc.user"))
    prop.setProperty("password",load.getString("jdbc.password"))
    tup.toDF("provincename","cityname","count(1)").write.mode("append").jdbc(load.getString("jdbc.url"),load.getString("jdbc.TableName"),prop)
    //tup.toDF("provincename","cityname","count(1)").coalesce(1).write.json("dir4/")
  }
}

package com.Rpt

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.utils.{JDBCConnectePools02, RptUtils}
import org.apache.spark.sql.{Dataset, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}


//地域分布指标
object LocationRpt {
  def main(args: Array[String]): Unit = {


    System.setProperty("hadoop.home.dir", "/Users/H/hadoop/bin")
    // 判断路径是否正确
    /*if(args.length != 1){
      println("目录参数不正确，退出程序")
      sys.exit()
    }*/
    // 创建一个集合保存输入和输出目录
    val Array(inputPath) = Array("/Users/H/Documents/IDEA_Projects/GP_22_DMP_tea/out/part-r-00000-9eac08dc-694a-4654-9ad1-777fc288754d.snappy.parquet")
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
      // 设置序列化方式 采用Kyro序列化方式，比默认序列化方式性能高
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    // 创建执行入口
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)
    // 获取数据
    val df = sQLContext.read.parquet(inputPath)
    // 将数据进行处理，统计各个指标

    import sQLContext.implicits._

    val res1= df.map(row => {
      // 把需要的字段全部取到
      val requestmode = row.getAs[Int]("requestmode")
      val processnode = row.getAs[Int]("processnode")
      val iseffective = row.getAs[Int]("iseffective")
      val isbilling = row.getAs[Int]("isbilling")
      val isbid = row.getAs[Int]("isbid")
      val iswin = row.getAs[Int]("iswin")
      val adorderid = row.getAs[Int]("adorderid")
      val WinPrice = row.getAs[Double]("winprice")
      val adpayment = row.getAs[Double]("adpayment")
      // key 值  是地域的省市
      val pro = row.getAs[String]("provincename")
      val city = row.getAs[String]("cityname")
      // 创建三个对应的方法处理九个指标
      val req = RptUtils.request(requestmode: Int, processnode: Int)
      val cli = RptUtils.click(requestmode: Int, iseffective: Int)
      val ad: List[Double] = RptUtils.Ad(iseffective: Int, isbilling: Int, isbid: Int, iswin: Int,
        adorderid: Int, WinPrice: Double, adpayment: Double)

      ((pro, city), req ++ cli ++ ad)
    })


    println(res1.collect().toBuffer)

    val res2 = res1.rdd.reduceByKey((x, y) => {
      x.zip(y).map(t => t._1 + t._2)
    })
    println(res2.collect().toBuffer)

    val res3= res2.map(x => {
      val pro = x._1._1
      val city = x._1._2
      val orinRequest: Double = x._2(0)
      val effectRequest: Double = x._2(1)
      val adRequest: Double = x._2(2)
      val showcnt: Double = x._2(3)
      val clickcnt: Double = x._2(4)
      val joinBid: Double = x._2(5)
      val bidWin: Double = x._2(6)
      val DSPconsume: Double = x._2(7)
      val DSPcost: Double = x._2(8)
      val clickpercent: Double = clickcnt / showcnt
     (pro, city, effectRequest, adRequest, joinBid, bidWin, showcnt, clickcnt, clickpercent, DSPcost, DSPconsume)
    })
    //println(res3.collect().toBuffer)
    res3.foreachPartition(myFun)
      sc.stop()




  }
    def myFun(iterator: Iterator[(String,String,Double,Double,Double,Double,Double,Double,Double,Double,Double)]): Unit = {
      var conn: Connection = null
      var ps: PreparedStatement = null
      val sql = "insert into procity(pro, city, effectRequest, adRequest, joinBid, bidWin, showcnt, clickcnt, clickpercent, DSPcost, DSPconsume) values (?,?,?,?,?,?,?,?,?,?,?)"
      try {
        conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "root")
        iterator.foreach(x => {
          ps = conn.prepareStatement(sql)
          ps.setString(1, x._1)
          ps.setString(2, x._2)
          ps.setDouble(3, x._3)
          ps.setDouble(4, x._4)
          ps.setDouble(5, x._5)
          ps.setDouble(6, x._6)
          ps.setDouble(7, x._7)
          ps.setDouble(8, x._8)
          ps.setDouble(9, x._9)
          ps.setDouble(10, x._10)
          ps.setDouble(11, x._11)
          ps.executeUpdate()
        }
        )
      } catch {
        case e: Exception => println("Mysql Exception")
      } finally {
        if (ps != null) {
          ps.close()
        }
        if (conn != null) {
          conn.close()
        }
      }
    }


    case class LocationDistribution(pro:String,city:String,effectRequest:Double,adRequest:Double,joinBid:Double,bidWin:Double,showcnt:Double,clickcnt:Double,clickpercent:Double,DSPcost:Double,DSPconsume:Double)
}



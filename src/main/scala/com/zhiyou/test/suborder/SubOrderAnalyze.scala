package com.zhiyou.test.suborder

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object SubOrderAnalyze {

  //设置SparkConf, Mater, AppName
  val conf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("diyige")
  //构建SparkContext
  val sc = new SparkContext(conf)

  def main(args: Array[String]): Unit = {
    //读取phonesub信息, 手机号所属区域
    val phoneResource = sc.textFile("file:///C:\\Users\\Administrator\\Desktop\\test\\phonesub")

    //通过phoneResource构建phoneRdd

    //    1,"1330000","广西 南宁","电信CDMA卡","0771","530000"
    //    3,"1330002","重庆 重庆","电信CDMA卡","023","400000"
    //    4,"1330003","重庆 重庆","电信CDMA卡","023","400000"
    val phoneRdd = phoneResource.map(x => {
      //通过逗号进行分割
      val info = x.split(",")
      //添加的两个字段phoneSub手机号区域, province手机的省份
      val phoneSub = info(1).replace("\"", "")
      val province = info(2).replace("\"", "").split("\\s")(0)
      //获取服务商的信息
      val telecom = info(3) match {
        case x if x.indexOf("移动") > 0 => "移动"
        case x if x.indexOf("联通") > 0 => "联通"
        case x if x.indexOf("电信") > 0 => "电信"
        case _ => "其他"
      }
      //我们只需要手机号段, 和省份信息, 服务商名称
      (phoneSub, (province, telecom))
    })

    //读取userorder中的信息, 用户信息, 手机号, 性别等等
    val orderResource =
      sc.textFile("file:///C:\\Users\\Administrator\\Desktop\\test\\userorder")

    //    8eeef6c7-155c-4e1e-a887-7a9ffaf96256
    //    |13301132682|f|23|29e8e8f7-4205-4bd0-897a-6d6b7fa66a4e
    //    8eeef6c7-155c-4e1e-a887-7a9ffaf96256
    //    |13301132682|f|23|997fa38f-4046-496e-83ba-751a4c45da6f
    //    8eeef6c7-155c-4e1e-a887-7a9ffaf96256
    //    |13301132682|f|23|0e8be0b4-240c-4eea-ac5f-091aeffb8822

    //通过split分割一行的数据
    // 对第二类数据进行分割, 只取, 手机号段, 作为键值
    val phoneSubOrder = orderResource.map(x => x.split("\\|"))
      .map(
        x => (x(1).substring(0, 7), x)
      )
      //通过手机号段, 连接上每个用户的手机省份, 和性别
      .leftOuterJoin(phoneRdd)
      // 此次计算只需要, 省份, 服务商名称, 用户标识id,


      //    8eeef6c7-155c-4e1e-a887-7a9ffaf96256
      //    |13301132682|f|23|0e8be0b4-240c-4eea-ac5f-091aeffb8822

      //province,telecom

      //左边是用户手机号段,       右边x._1 用户信息 , x._2 用户手机号区域信息
      //得到
      .map(x => {
      (x._2._2, x._2._1)
    })
      .mapValues(x => {
        (x(1), x(2), x(4))
        //    (Some((重庆,移动)),(15223663301,f,9f3f5467-104c-4031-a123-33c01d8df8ea))
        //    (Some((重庆,移动)),(15223667822,f,6b427cca-069f-469d-95c4-7bf00d39c8ef))
      })


      //电话号码	       性别          订单ID
      .map(x => {
      (x._1.getOrElse(("", "")), x._2._1, x._2._2)
//      ((广东,电信),18988596387,m)
//      ((广东,电信),18988596387,m)
    })
      .distinct()
      .map(x => {
        (x._1._1,(x._1._2, x._2, x._3))
      })
      .mapValues(x => {
        //总数据量, 移动数量, 电信数量, 联通数量, 男数量, 女数量
        (1
          , if(x._1=="移动") 1 else 0
          , if(x._1=="电信") 1 else 0
          , if(x._1=="联通") 1 else 0
          , if(x._1=="m") 1 else 0
          , if(x._1=="f") 1 else 0
        )
      })
//    (河南,(1,1,0,0,0,0))
//    (江西,(1,1,0,0,0,0))
        .reduceByKey((x1,x2) => {
      (
        x1._1+x2._1
        , x1._2+x2._2
        , x1._3+x2._3
        , x1._4+x2._4
        , x1._5+x2._5
        , x1._6+x2._6
      )
    })
      .mapValues(x => {
        (x._1
          , x._2*1.00/x._1
          , x._3*1.00/x._1
          , x._4*1.00/x._1
          , x._5*1.00/x._1
          , x._6*1.00/x._1)
      })






    phoneSubOrder.foreach(println)

  }


}

package com.zhiyou.test.suborder

import java.sql.Timestamp

import org.apache.spark.sql.SparkSession

object SubOrderAnalyze {

  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("spark read hdfs")
    .enableHiveSupport()
    .getOrCreate()

  import spark.implicits._
  import spark.sql
  //  手机号段信息表：phonesub
  //  号段ID    号段    地区        运营商         座机区号    邮编
  //  20    "1330019"   "上海 上海""电信CDMA卡"    "021"     "200000"
  //  6个字段
  // 以","分割

  def createPhoneSubFromHdfs() = {
    //spark读取hdfs上的文件, 生成DataFrame格式数据
    val df = spark.read.text("/phonesub")
    df.printSchema()

    //对ds进行字符串, 划分, 生成Dataset格式文件, dataset格式可以创建视图
    val ds = df.map(x => {
      val info = x.getString(0).split(",")
      (info(0), info(1), info(2), info(3), info(4), info(5))
    })

    ds.printSchema()
    ds.createOrReplaceTempView("phone_sub")


    //  号段ID phone_sub_id    号段  phone_sub          地区          运营商         座机区号    邮编
    //  20                    "1330019"   "上海 上海" "电信CDMA卡"    "021"     "200000"

    //号段ID  phone_sub_id
    //号段      phone_sub
    //    地区      pro_city
    //    运营商    agent
    //    座机区号   tel_sub
    //    邮编      postcode
    //
    val result = sql(
      """
        |create table if not exists test20171208.ods_phone_sub
        |as
        |select  _1 phone_sub_id
        |       , _2 phone_sub
        |       , _3 pro_city
        |       , _4 agent
        |       , _5 tel_sub
        |       , _6 postcode
        |from phone_sub
      """.stripMargin
    )

    println("phone_sub表")

    result.printSchema()
    result.show()

  }


  //
  //  订单信息表：  userorder
  //  用户id
  //  电话号码
  //  性别
  //  年龄
  //  订单ID

  //6 个字段, 以  |  分割
  def createUserOrderFromHdfs() = {
    //spark读取hdfs上的文件, 生成DataFrame格式数据
    val df = spark.read.text("/userorder")
    df.printSchema()

    //对ds进行字符串, 划分, 生成Dataset格式文件, dataset格式可以创建视图
    val ds = df.map(x => {
      val info = x.getString(0).split("\\|")
      (info(0), info(1), info(2), info(3), info(4))
    })

    ds.printSchema()
    ds.createOrReplaceTempView("user_order")




    //号段ID  phone_sub_id
    //号段      phone_sub
    //    地区      pro_city
    //    运营商    agent
    //    座机区号   tel_sub
    //    邮编      postcode


    //  订单信息表：userorder
    //  用户id    user_id
    //  电话号码    phone
    //  性别        sex
    //  年龄        age
    //  订单ID      order_id
    val result = sql(
      """
        |create table if not exists test20171208.ods_user_order
        |as
        |select  _1 user_id
        |       , _2 phone
        |       , _3 sex
        |       , _4 age
        |       , _5 order_id
        |from user_order
      """.stripMargin
    )

    println("user_order表")

    result.printSchema()
    result.show()

  }


  def main(args: Array[String]): Unit = {
    createPhoneSubFromHdfs()
    createUserOrderFromHdfs()


    //
    sql(
      """
        |create table if not exists test20171208.dwd_user_order
        |as
        |select   user_id
        |       ,  phone
        |       ,  sex
        |       ,  age
        |       ,  order_id
        |       , substring(phone,1,7) phone_sub
        |from test20171208.ods_user_order
        |
      """.stripMargin
    )

    sql(
      """
        |create table if not exists test20171208.dwd_phone_sub
        |as
        |select
        |	phone_sub_id
        |	,regexp_replace(phone_sub,"\"","") phone_sub
        |	,regexp_replace(pro_city,"\"","") pro_city
        |	,regexp_replace(agent,"\"","") agent
        |	,regexp_replace(tel_sub,"\"","") tel_sub
        |	,regexp_replace(postcode,"\"","") postcode
        |	,split(regexp_replace(pro_city,"\"","")," ")[0] province
        |	,substring( regexp_replace(agent,"\"","" ),1,2) agent0
        |from test20171208.ods_phone_sub
        |
      """.stripMargin
    )

    val result = sql(
      """
        |select 	b.province
        |		,count(user_id) user_num
        |		,sum(case when b.agent0='电信' then 1 else 0 end)/count(b.agent0) dianxin
        |		,sum(case when b.agent0='移动' then 1 else 0 end)/count(b.agent0) dianxin
        |		,sum(case when b.agent0='联通' then 1 else 0 end)/count(b.agent0) dianxin
        |		,sum(case when a.sex='m' then 1 else 0 end)/count(a.sex) man
        |		,sum(case when a.sex='f' then 1 else 0 end)/count(a.sex) woman
        |from test20171208.dwd_user_order a
        |	left join test20171208.dwd_phone_sub b
        |	on a.phone_sub=b.phone_sub
        |group by b.province
        |
      """.stripMargin
    )

    result.show()


  }


}

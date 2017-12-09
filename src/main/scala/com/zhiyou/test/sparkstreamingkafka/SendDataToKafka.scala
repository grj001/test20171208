package com.zhiyou.test.sparkstreamingkafka

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Random


case class HardwareInfo(datetime: String, harewareType: String, occRate: Int)

object SendDataToKafka {

  /*
  *
  * 2017-12-01 11:11:11|cpu|85
    2017-12-01 11:11:12|主板|88
    2017-12-01 11:11:13|网络|20
  * */
  val random = new Random()

  //    val hotelId = random.nextInt(20) + 1
  //    val platforms = List("携程", "艺龙", "去哪儿", "大众点评", "美团")
  //    val userIds = (1 to 100).map(x => (x, s"用户姓名${x}"))
  //    val userLevels = List("新手", "初级", "中级", "高级")
  val dateFormater = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  val datetime = new Timestamp(
    dateFormater.parse("2017-07-17 00:00:00").getTime + 1000 + random.nextInt(60 * 1000))

  //cpu,主板,内存,网络,磁盘
  val harewareTypes = List("cpu", "主板", "网络", "磁盘")
  val occRate = random.nextInt(99) + 1

  def mkRandomJude() = {



    (1 to random.nextInt(100)).foreach(x => {

      val datetime = new Timestamp(
        (dateFormater.parse(
          "2017-07-17 00:00:00"
        ).getTime
          + 1000
          + random.nextInt(60 * 1000))/100
      )
      val harewareType = harewareTypes(random.nextInt(4))
      val occRate = random.nextInt(99) + 1

      println(s"""$datetime|$harewareType|$occRate""")
    })

  }


  def main(args: Array[String]): Unit = {
    //      val client = FlumeClient("master", 9999);
    //      (1 to 100).foreach(i => {
    //        val msg = mkRandomJude().toString
    //        client.sendString(msg)
    //        Thread.sleep(1000)
    //      })
    //      client.close()
    mkRandomJude()
  }

}
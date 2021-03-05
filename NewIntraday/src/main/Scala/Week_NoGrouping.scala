import java.sql.Date
import java.text.SimpleDateFormat

import Bean.WeekAll
import org.apache.spark.sql.SparkSession

/**
 * 不分类型进行计算
 *
 *
 *
 *
 * 临时计算的三个需求
 *
 * 反思： 刚开始思路没有理清楚。从读取数据开始进行数据的计算，在计算之后发现没有办法按照基金名字进行分类，此时想的是加一个基金名字的字段。
 *
 * 因为我已经有个总表了，所以直接就按照总表进行数据读取计算就行了。
 * 至于 group by 之后的字段，明确group只是需要 求count数就行了，现在是已经有了count数，直接用常数代替就行了
 *
 *
 * 拿到需求先从头到尾梳理一下，不要盲目不要盲目
 *
 */

object Week_NoGrouping {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .config("spark.driver.host", "localhost")
      .appName("sql")
      .getOrCreate()
    import spark.sqlContext.implicits._


    //一周的汇总之后的数据路径
    val week_date = "D:\\DATA\\2.22-2.26\\summary.txt"


    val lines = spark.sparkContext.
      textFile(week_date).map(_.split("\t"))

    val emp = lines.map(x => WeekAll(x(0), x(1),
      new Date(new SimpleDateFormat("yyyy/MM/dd").parse(x(2)).getTime),
      x(3).toFloat,
      x(4).toFloat,
      x(5).toFloat,
      x(6).toFloat,
      x(7).toFloat,
      x(8).toFloat,
      x(9).toFloat,
      x(10).toFloat,
      x(11).toFloat,
      x(12).toFloat,
      x(13).toFloat))
    val frame = emp.toDF()
    frame.show()
    frame.createTempView("summary")


    /**
     *
     * 需求一：涨幅相反
     */

    spark.sql(
      """
        |
        |select
        |count(*)  as `基金数量` ,
        |sum(countmy)  as `蚂蚁相反个数`,
        |concat(cast((sum(countmy) / count(*)) * 100 as decimal(16,3)),'%')as `蚂蚁相反估计占比`,
        |sum(counttt)  as `天天相反个数`,
        |concat(cast((sum(counttt) / count(*)) * 100 as decimal(16,3)),'%') as `天天相反估计占比`,
        |sum(countjy)  as `聚源相反个数`,
        |concat(cast((sum(countjy) / count(*)) * 100 as decimal(16,3)),'%' )as `聚源相反估计占比`,
        |sum(counthm)  as `好买相反个数`,
        |concat(cast((sum(counthm) / count(*)) * 100 as decimal(16,3)),'%') as `好买相反估计占比`
        |
        |from(
        |select
        |report_return,
        |fund_code,
        |time_int,
        |if(summary.report_return * cast (summary.estimate_ratio as float ) <0,1,0 ) countmy,
        |if(summary.report_return * summary.jy_return <0,1,0 ) countjy,
        |if(summary.report_return * summary.Fzdbl <0,1,0 ) counthm,
        |if(summary.report_return * summary.tiantian_return <0,1,0 ) counttt
        |from summary ) ptagegain
        |
        |""".stripMargin)

    /*.coalesce(1)
    .write.mode("Append")
    .option("header", "true")
    .format("CSV")
    .save("D:\\Result\\Main\\Week_NoGrouping\\need1_opposite")*/.show()


    /**
     * 自定义 绝对值的函数 abs 并且进行注册为abs
     *
     * @param a
     * @return
     */
    def abs(a: Float): Float = {
      if (a > 0)
        a
      else
        -a
    }

    spark.udf.register("abs", abs _)

    /**
     * 注册自定义UDAF 求中值的函数 median
     */
    spark.udf.register("median", new Median())


    /**
     * 2. 估值绝对偏差的均值（x1000）、中位数（x1000）、标准差（x1000）
     * 估值绝对偏差x1000=abs（各平台估算净值-实际净值）x1000
     */


    /**
     * 2.1 估值绝对偏差的均值（x1000）
     *
     * 估值绝对偏差均值x1000= average【abs（各平台估算净值-实际净值）】=sum【abs（各平台估算净值-实际净值）】/n x1000
     */

    spark.sql(
      """
        |select
        |
        |count(fund_code) as `基金数量`,
        |cast(sum(abs(summary.estimate_return -summary.report_nav)) / count(fund_code) * 1000 as decimal(16,3)) as `蚂蚁估值绝对偏差均值`,
        |cast(sum(abs(summary.tiantian_nav    -summary.report_nav)) / count(fund_code) * 1000 as decimal(16,3)) as `天天估值绝对偏差均值`,
        |cast(sum(abs(summary.jy_nav          -summary.report_nav)) / count(fund_code) * 1000 as decimal(16,3)) as `聚源估值绝对偏差均值`,
        |cast(sum(abs(summary.Fgz             -summary.report_nav)) / count(fund_code) * 1000 as decimal(16,3)) as `好买估值绝对偏差均值`,
        |cast(sum(abs(WDestimate_return       -summary.report_nav)) / count(fund_code) * 1000 as decimal(16,3)) as `万德估值绝对偏差均值`
        |from summary
        |
        |""".stripMargin)
      /*.coalesce(1)
      .write.mode("Append")
      .option("header", "true")
      .format("CSV")
      .save("D:\\Result\\Main\\Week_NoGrouping\\\\need2_absolute_mean")*/.show()


    /**
     * 2.2 估值绝对偏差的中位数（x1000）： t30-t33  wind t150
     * 估值绝对偏差中值：估值偏差按升序或者降序排列，假如有n个数据，
     * 当n为偶数时，中位数为第n/2位数和第(n+2)/2位数的平均数；如果n为奇数，那么中位数为第（n+1）/2位数的值，后放大x1000
     */


    spark.sql(
      """
        |select
        |
        |my.c           as `基金数量` ,
        |my.Myvaluation as `蚂蚁估值绝对偏差中值`,
        |tt.Ttvaluation as `天天估值绝对偏差中值`,
        |jy.Jyvaluation as `聚源估值绝对偏差中值`,
        |hm.Hmvaluation as `好买估值绝对偏差中值`,
        |wd.Wdvaluation as `万德估值绝对偏差中值`
        |
        |from(
        |select
        |count(t30.fund_code) c ,
        |cast(median(MyMedian) as decimal (16,3))  as Myvaluation
        |from(
        |select
        |summary.fund_code,
        |cast (abs(summary.estimate_return - summary.report_nav) *1000 as decimal(16,3)) MyMedian
        |from summary
        |order by MyMedian ) t30
        |) my
        |
        |join
        |(select
        |count(t31.fund_code) as c1,
        |cast(median(JyMedian) as decimal(16,3))as Jyvaluation
        |from(
        |select
        |summary.fund_code,
        |cast (abs(summary.jy_nav - summary.report_nav) *1000 as decimal(16,3)) JyMedian
        |from summary
        |order by JyMedian ) t31
        |) jy
        |on my.c = jy.c1
        |
        |join
        |(select
        |
        |count(t32.fund_code) as c2,
        |cast(median(HmMedian) as decimal(16,3)) as Hmvaluation
        |from (
        |select
        |summary.fund_code,
        |cast (abs(summary.Fgz - summary.report_nav) *1000 as decimal(16,3)) HmMedian
        |from summary
        |order by HmMedian ) t32
        | ) hm
        |on my.c = hm.c2
        |
        |
        |join
        |(select
        |count(t150.fund_code) as c6,
        |cast(median(WdMedian) as decimal(16,3)) as Wdvaluation
        |from (
        |select
        |summary.fund_code,
        |cast (abs(summary.WDestimate_return - summary.report_nav) *1000 as decimal(16,3)) WdMedian
        |from summary
        |order by WdMedian ) t150
        | ) wd
        |on my.c = wd.c6
        |
        |join
        |(select
        |count(t33.fund_code) as c3,
        |cast(median(TtMedian) as decimal(16,3)) as Ttvaluation
        |from(
        |select
        |summary.fund_code,
        |cast (abs(summary.tiantian_nav - summary.report_nav) *1000 as decimal(16,3)) TtMedian
        |from summary
        |order by TtMedian ) t33
        | ) tt
        |on my.c = tt.c3
        |
        |
        |
        |""".stripMargin)
      /*.coalesce(1)
      .write.mode("Append")
      .option("header", "true")
      .format("CSV")
      .save("D:\\Result\\Main\\Week_NoGrouping\\need2_absolute_median")*/ .show()


    /**
     * 2.3  估值绝对偏差的标准差（x1000） t40 - t43  wind t151
     *
     */
    spark.sql(
      """
        |
        |select
        |
        |
        |mayi.count       as `基金数量` ,
        |mayi.MyStd       as `蚂蚁估值绝对偏差标准差`,
        |tiantian.TtStd   as `天天估值绝对偏差标准差`,
        |juyuan.JyStd     as `聚源估值绝对偏差标准差`,
        |haomai.HmStd     as `好买估值绝对偏差标准差`,
        |wind.WdStd       as `万德估值绝对偏差标准差`
        |
        |from
        |(select
        |count(*) count,
        |cast (stddev_pop(Myadv)  as decimal(16,3)) as MyStd
        |from(
        |select
        |cast (abs(summary.estimate_return - summary.report_nav) *1000 as decimal(16,3)) Myadv
        |from  summary) t40
        |) mayi
        |
        |join
        |(select
        |count(*) count1,
        |cast (stddev_pop(Jyadv)  as decimal(16,3)) as JyStd
        |from(
        |select
        |cast (abs(summary.jy_nav - summary.report_nav) *1000 as decimal(16,3)) Jyadv
        |from  summary) t41
        |) juyuan
        |on mayi.count = juyuan.count1
        |
        |join
        |(select
        |count(*) count2,
        |cast (stddev_pop(Hmadv)  as decimal(16,3))  HmStd
        |from(
        |select
        |cast (abs(summary.Fgz - summary.report_nav) *1000 as decimal(16,3)) Hmadv
        |from  summary) t42
        |) haomai
        |on mayi.count = haomai.count2
        |
        |join
        |(select
        |count(*) count3,
        |cast (stddev_pop(Ttadv)  as decimal(16,3))  TtStd
        |from(
        |select
        |cast (abs(summary.tiantian_nav - summary.report_nav) *1000 as decimal(16,3)) Ttadv
        |from  summary) t43
        |) tiantian
        |on mayi.count = tiantian.count3
        |
        |
        |join
        |(select
        |count(*) count6,
        |cast (stddev_pop(Wdadv)  as decimal(16,3))  WdStd
        |from(
        |select
        |cast (abs(summary.WDestimate_return - summary.report_nav) *1000 as decimal(16,3)) Wdadv
        |from  summary) t151
        |) wind
        |on mayi.count = wind.count6
        |
        |
        |""".stripMargin)
      /*.coalesce(1)
      .write.mode("Append")
      .option("header", "true")
      .format("CSV")
      .save("D:\\Result\\Main\\Week_NoGrouping\\need2_absolute_std")*/ .show()


    /**
     *
     *
     *
     * 3.估值相对偏差的均值（x1000）、中位数（x1000）、标准差（x1000）
     * 估值相对偏差=abs（各平台估算净值-实际净值）/实际净值
     */
    /**
     * 3.1 估值相对偏差的均值（x1000）
     */

    spark.sql(
      """
        |
        |select
        |
        |count(*) as `基金数量`,
        |concat(cast( sum(  abs((summary.estimate_return     -summary.report_nav) / (summary.report_nav)) ) / count(*) *100 as decimal(16,3)),'%' )as `蚂蚁估值相对偏差均值`,
        |concat(cast( sum(  abs((summary.tiantian_nav        -summary.report_nav) / (summary.report_nav)) ) / count(*) *100 as decimal(16,3)),'%' )as `天天估值相对偏差均值`,
        |concat(cast( sum(  abs((summary.jy_nav              -summary.report_nav) / (summary.report_nav)) ) / count(*) *100 as decimal(16,3)),'%' )as `聚源估值相对偏差均值`,
        |concat(cast( sum(  abs((summary.Fgz                 -summary.report_nav) / (summary.report_nav)) ) / count(*) *100 as decimal(16,3)),'%' )as `好买估值相对偏差均值`,
        |concat(cast( sum(  abs((summary.WDestimate_return   -summary.report_nav) / (summary.report_nav)) ) / count(*) *100 as decimal(16,3)),'%' )as `万德估值相对偏差均值`
        |from summary
        |
        |""".stripMargin)
      /*.coalesce(1)
      .write.mode("Append")
      .option("header", "true")
      .format("CSV")
      .save("D:\\Result\\Main\\Week_NoGrouping\\need3_relative_mean")*/ .show()





    //中值
    /**
     * 中值  完成
     */
    spark.sql(
      """
        |
        |
        |
        |
        |select
        |
        |my.c1  as  `基金数量`,
        |concat(my.Myvaluation1,'%')     as  `蚂蚁估值相对偏差中值`,
        |concat(tt.Ttvaluation1,'%')     as  `天天估值相对偏差中值`,
        |concat(jy.Jyvaluation1,'%')     as  `聚源估值相对偏差中值`,
        |concat(hm.Hmvaluation1,'%')     as  `好买估值相对偏差中值`,
        |concat(wind.Wdvaluation1,'%')   as  `万德估值相对偏差中值`
        |
        |from
        |(select
        |
        |count(*) c1,
        |cast(median(MyMedian1) as decimal(16,3) ) Myvaluation1
        |from(
        |select
        |summary.fund_code,
        |summary.type     ,
        |cast (abs((summary.estimate_return   -summary.report_nav)/(summary.report_nav)) *100 as decimal(16,3)) MyMedian1
        |from summary
        |order by MyMedian1) a) my
        |join
        |
        |(select
        |count(*) c2,
        |cast(median(JytMedian1) as decimal(16,3) ) Jyvaluation1
        |from(
        |select
        |summary.fund_code,
        |summary.type     ,
        |cast (abs((summary.jy_nav   -summary.report_nav)/(summary.report_nav)) *100 as decimal(16,3)) JytMedian1
        |from summary
        |order by JytMedian1) b) jy
        |on my.c1=jy.c2
        |
        |join(
        |select
        |count(*) c3,
        |cast(median(HmMedian1) as decimal(16,3) ) Hmvaluation1
        |from(
        |select
        |summary.fund_code,
        |summary.type     ,
        |cast (abs((summary.Fgz   -summary.report_nav)/(summary.report_nav)) *100 as decimal(16,3)) HmMedian1
        |from summary
        |order by HmMedian1) c) hm
        |on my.c1=hm.c3
        |
        |join(
        |select
        |count(*) c4,
        |cast(median(TtMedian1) as decimal(16,3) ) Ttvaluation1
        |from(
        |select
        |summary.fund_code,
        |summary.type     ,
        |cast (abs((summary.tiantian_nav   -summary.report_nav)/(summary.report_nav)) *100 as decimal(16,3)) TtMedian1
        |from summary
        |order by TtMedian1) d) tt
        |on my.c1=tt.c4
        |
        |join(
        |select
        |count(*) c6,
        |cast(median(WdMedian1) as decimal(16,3) ) Wdvaluation1
        |from(
        |select
        |summary.fund_code,
        |summary.type     ,
        |cast (abs((summary.WDestimate_return   -summary.report_nav)/(summary.report_nav)) *100 as decimal(16,3)) WdMedian1
        |from summary
        |order by WdMedian1) f) wind
        |on my.c1=wind.c6
        |
        |""".stripMargin)
      /*.coalesce(1)
      .write.mode("Append")
      .option("header", "true")
      .format("CSV")
      .save("D:\\Result\\Main\\Week_NoGrouping\\need3_relative_median")*/.show()


    /**
     * 标准差 完成
     */
    spark.sql(
      """
        |select
        |
        |mayi1.co                       as  `基金数量`,
        |concat(mayi1.MyStd1     , '%') as `蚂蚁估值相对偏差标准差` ,
        |concat(tiantian1.TtStd1 , '%') as `天天估值相对偏差标准差` ,
        |concat(juyuan1.JyStd1   , '%') as `聚源估值相对偏差标准差` ,
        |concat(haomai1.HmStd1   , '%') as `好买估值相对偏差标准差` ,
        |concat(wind1.WdStd1     , '%') as `万德估值相对偏差标准差`
        |from(
        |select
        |count(*) co ,
        |cast (stddev_pop(Myadv1)  as decimal(16,3)) MyStd1
        |from(
        |select
        |summary.type    ,
        |cast (abs((summary.estimate_return -summary.report_nav)/(summary.report_nav)) *100 as decimal(16,3)) Myadv1
        |from  summary) t45) mayi1
        |
        |join
        |(
        |select
        |count(*) co1 ,
        |cast (stddev_pop(Jyadv1)  as decimal(16,3)) JyStd1
        |from(
        |select
        |summary.type    ,
        |cast (abs((summary.jy_nav -summary.report_nav)/(summary.report_nav)) *100 as decimal(16,3)) Jyadv1
        |from  summary) t45) juyuan1
        |
        |on mayi1.co = juyuan1.co1
        |
        |join(
        |select
        |count(*) co2 ,
        |cast (stddev_pop(Hmadv1)  as decimal(16,3)) HmStd1
        |from(
        |select
        |summary.type    ,
        |cast (abs((summary.Fgz -summary.report_nav)/(summary.report_nav)) *100 as decimal(16,3)) Hmadv1
        |from  summary) t45)haomai1
        |on mayi1.co = haomai1.co2
        |
        |
        |join (select
        |count(*) co3 ,
        |cast (stddev_pop(Ttadv1)  as decimal(16,3)) TtStd1
        |from(
        |select
        |summary.type    ,
        |cast (abs((summary.tiantian_nav -summary.report_nav)/(summary.report_nav)) *100 as decimal(16,3)) Ttadv1
        |from  summary) t46)tiantian1
        |on mayi1.co = tiantian1.co3
        |
        |join (select
        |count(*) co6 ,
        |cast (stddev_pop(Wdadv1)  as decimal(16,3)) WdStd1
        |from(
        |select
        |summary.type    ,
        |cast (abs((summary.WDestimate_return -summary.report_nav)/(summary.report_nav)) *100 as decimal(16,3)) Wdadv1
        |from  summary) t152)wind1
        |on mayi1.co = wind1.co6
        |
        |""".stripMargin)
      /*.coalesce(1)
      .write.mode("Append")
      .option("header", "true")
      .format("CSV")
      .save("D:\\Result\\Main\\Week_NoGrouping\\need3_relative_std")*/.show()


    /**
     *
     * 4.a. 统计估值绝对偏差落在以下区间内的个数及占比（该区间的个数/该日总基金的个数） t10-t13 wind t154 z
     * <=0.001	（0.001,0.003]	(0.003,0.005]	(0.005,0.01]	(0.01,0.02]	 >0.02
     */
    spark.sql(
      """
        |
        |
        |select
        |
        |'蚂蚁'      as `基金名称`,
        |concat(cast(sum(a1) / count(*) * 100 as decimal(16,3)),'%') as `<=0.001`,
        |concat(cast(sum(a2) / count(*) * 100 as decimal(16,3)),'%') as `(0.001,0.003]`,
        |concat(cast(sum(a3) / count(*) * 100 as decimal(16,3)),'%') as `(0.003,0.005]`,
        |concat(cast(sum(a4) / count(*) * 100 as decimal(16,3)),'%') as `(0.005,0.01]`,
        |concat(cast(sum(a5) / count(*) * 100 as decimal(16,3)),'%') as `(0.01,0.02]`,
        |concat(cast(sum(a6) / count(*) * 100 as decimal(16,3)),'%') as `>0.02`
        |from(
        |select
        |summary.fund_code  ,
        |if( abs(summary.estimate_return - summary.report_nav) <=0.001,1,0) a1,
        |if((abs(summary.estimate_return - summary.report_nav) between 0.001 and  0.003), 1,0) a2,
        |if((abs(summary.estimate_return - summary.report_nav) between 0.003 and  0.005), 1,0) a3,
        |if((abs(summary.estimate_return - summary.report_nav) between 0.005 and  0.01 ), 1,0) a4,
        |if((abs(summary.estimate_return - summary.report_nav) between 0.01  and  0.02 ), 1,0) a5,
        |if (abs(summary.estimate_return - summary.report_nav)>0.02,1,0) a6
        |from summary) t10
        |
        |union all
        |select
        |
        |'天天'        as `基金名称`,
        |concat(cast(sum(d1) / count(*) * 100 as decimal(16,3)),'%')as `<=0.001`,
        |concat(cast(sum(d2) / count(*) * 100 as decimal(16,3)),'%')as `（0.001,0.003]`,
        |concat(cast(sum(d3) / count(*) * 100 as decimal(16,3)),'%')as `(0.003,0.005]`,
        |concat(cast(sum(d4) / count(*) * 100 as decimal(16,3)),'%')as `(0.005,0.01]`,
        |concat(cast(sum(d5) / count(*) * 100 as decimal(16,3)),'%')as `(0.01,0.02]`,
        |concat(cast(sum(d6) / count(*) * 100 as decimal(16,3)),'%')as `>0.02`
        |from(
        |select
        |summary.fund_code,

        |if( abs(summary.tiantian_nav - summary.report_nav)<=0.001,1,0) d1,
        |if((abs(summary.tiantian_nav - summary.report_nav) between 0.001 and  0.003), 1,0) d2,
        |if((abs(summary.tiantian_nav - summary.report_nav) between 0.003 and  0.005), 1,0) d3,
        |if((abs(summary.tiantian_nav - summary.report_nav) between 0.005 and  0.01 ), 1,0)  d4,
        |if((abs(summary.tiantian_nav - summary.report_nav) between 0.01  and  0.02 ), 1,0) d5,
        |if( abs(summary.tiantian_nav - summary.report_nav)>0.02,1,0) d6
        |from summary) t13
        |
        |
        |union all
        |select
        |'聚源'          as `基金名称`,
        |concat(cast(sum(b1) / count(*) *100 as decimal(16,3)),'%') as `<=0.001`,
        |concat(cast(sum(b2) / count(*) *100 as decimal(16,3)),'%') as `（0.001,0.003]`,
        |concat(cast(sum(b3) / count(*) *100 as decimal(16,3)),'%') as `(0.003,0.005]`,
        |concat(cast(sum(b4) / count(*) *100 as decimal(16,3)),'%') as `(0.005,0.01]`,
        |concat(cast(sum(b5) / count(*) *100 as decimal(16,3)),'%') as `(0.01,0.02]`,
        |concat(cast(sum(b6) / count(*) *100 as decimal(16,3)),'%') as `>0.02`
        |from(
        |select
        |summary.fund_code,
        |if( abs(summary.jy_nav - summary.report_nav)<=0.001,1,0) b1,
        |if((abs(summary.jy_nav - summary.report_nav) between 0.001 and  0.003), 1,0) b2,
        |if((abs(summary.jy_nav - summary.report_nav) between 0.003 and  0.005), 1,0) b3,
        |if((abs(summary.jy_nav - summary.report_nav) between 0.005 and  0.01 ), 1,0) b4,
        |if((abs(summary.jy_nav - summary.report_nav) between 0.01  and  0.02 ), 1,0) b5,
        |if( abs(summary.jy_nav - summary.report_nav)>0.02,1,0) b6
        |from summary) t11
        |
        |union all
        |select
        |
        |'好买'        as `基金名称`,
        |concat(cast(sum(c1) / count(*) *100 as decimal(16,3)),'%') as `<=0.001`,
        |concat(cast(sum(c2) / count(*) *100 as decimal(16,3)),'%') as `（0.001,0.003]`,
        |concat(cast(sum(c3) / count(*) *100 as decimal(16,3)),'%') as `(0.003,0.005]`,
        |concat(cast(sum(c4) / count(*) *100 as decimal(16,3)),'%') as `(0.005,0.01]`,
        |concat(cast(sum(c5) / count(*) *100 as decimal(16,3)),'%') as `(0.01,0.02]`,
        |concat(cast(sum(c6) / count(*) *100 as decimal(16,3)),'%') as `>0.02`
        |from(
        |select
        |summary.fund_code,
        |if( abs(summary.Fgz - summary.report_nav)<=0.001,1,0) c1,
        |if((abs(summary.Fgz - summary.report_nav) between 0.001 and  0.003), 1,0) c2,
        |if((abs(summary.Fgz - summary.report_nav) between 0.003 and  0.005), 1,0) c3,
        |if((abs(summary.Fgz - summary.report_nav) between 0.005 and  0.01 ), 1,0) c4,
        |if((abs(summary.Fgz - summary.report_nav) between 0.01  and  0.02 ), 1,0) c5,
        |if( abs(summary.Fgz - summary.report_nav)>0.02,1,0) c6
        |from summary) t12
        |
        |
        |union all
        |select
        |'万德'        as `基金名称`,
        |concat(cast(sum(z1) / count(*) *100 as decimal(16,3)),'%') as `<=0.001`,
        |concat(cast(sum(z2) / count(*) *100 as decimal(16,3)),'%') as `（0.001,0.003]`,
        |concat(cast(sum(z3) / count(*) *100 as decimal(16,3)),'%') as `(0.003,0.005]`,
        |concat(cast(sum(z4) / count(*) *100 as decimal(16,3)),'%') as `(0.005,0.01]`,
        |concat(cast(sum(z5) / count(*) *100 as decimal(16,3)),'%') as `(0.01,0.02]`,
        |concat(cast(sum(z6) / count(*) *100 as decimal(16,3)),'%') as `>0.02`
        |from(
        |select
        |summary.fund_code,
        |if( abs(summary.WDestimate_return - summary.report_nav)<=0.001,1,0) z1,
        |if((abs(summary.WDestimate_return - summary.report_nav) between 0.001 and  0.003), 1,0) z2,
        |if((abs(summary.WDestimate_return - summary.report_nav) between 0.003 and  0.005), 1,0) z3,
        |if((abs(summary.WDestimate_return - summary.report_nav) between 0.005 and  0.01 ), 1,0) z4,
        |if((abs(summary.WDestimate_return - summary.report_nav) between 0.01  and  0.02 ), 1,0) z5,
        |if( abs(summary.WDestimate_return - summary.report_nav)>0.02,1,0) z6
        |from summary) t154
        |
        |
        |""".stripMargin)
      /*.coalesce(1)
      .write.mode("Append")
      .option("header", "true")
      .format("CSV")
      .save("D:\\Result\\Main\\Week_NoGrouping\\need4_Absolute_Deviation1")*/.show()


    /**
     *
     * 4.b 统计估值绝对偏差落在以下区间内的个数及占比（该区间的个数/该日总基金的个数） t14-17  wind 155  zz
     *
     * <=0.001	<=0.003	<=0.005	<=0.01	 <=0.02  	>0.02
     *
     *
     */

    spark.sql(
      """
        |
        |
        |select
        |
        |'蚂蚁'         as `基金名称`,
        |concat(cast(sum(aa1) / count(*) *100 as decimal(16,3)),'%') as `<=0.001`,
        |concat(cast(sum(aa2) / count(*) *100 as decimal(16,3)),'%') as `<=0.003`,
        |concat(cast(sum(aa3) / count(*) *100 as decimal(16,3)),'%') as `<=0.005`,
        |concat(cast(sum(aa4) / count(*) *100 as decimal(16,3)),'%') as `<=0.01 `,
        |concat(cast(sum(aa5) / count(*) *100 as decimal(16,3)),'%') as `<=0.02 `,
        |concat(cast(sum(aa6) / count(*) *100 as decimal(16,3)),'%') as `>0.02 `
        |from(
        |select
        |summary.fund_code ,
        |if((abs(summary.estimate_return - summary.report_nav) <= 0.001 ), 1,0) aa1,
        |if((abs(summary.estimate_return - summary.report_nav) <= 0.003 ), 1,0) aa2,
        |if((abs(summary.estimate_return - summary.report_nav) <= 0.005 ), 1,0) aa3,
        |if((abs(summary.estimate_return - summary.report_nav) <= 0.01  ), 1,0) aa4,
        |if((abs(summary.estimate_return - summary.report_nav) <= 0.02  ), 1,0) aa5,
        |if((abs(summary.estimate_return - summary.report_nav) >  0.02  ), 1,0) aa6
        |from summary) t14
        |
        |union all
        |select
        |
        |'天天'         as `基金名称`,
        |concat(cast(sum(dd1) / count(*) * 100 as decimal(16,3)),'%') as `<=0.001`,
        |concat(cast(sum(dd2) / count(*) * 100 as decimal(16,3)),'%') as `<=0.003`,
        |concat(cast(sum(dd3) / count(*) * 100 as decimal(16,3)),'%') as `<=0.005`,
        |concat(cast(sum(dd4) / count(*) * 100 as decimal(16,3)),'%') as `<=0.01 `,
        |concat(cast(sum(dd5) / count(*) * 100 as decimal(16,3)),'%') as `<=0.02 `,
        |concat(cast(sum(dd6) / count(*) * 100 as decimal(16,3)),'%') as `>0.02 `
        |from(
        |select
        |summary.fund_code ,
        |if((abs(summary.tiantian_nav - summary.report_nav) <= 0.001 ), 1,0) dd1,
        |if((abs(summary.tiantian_nav - summary.report_nav) <= 0.003 ), 1,0) dd2,
        |if((abs(summary.tiantian_nav - summary.report_nav) <= 0.005 ), 1,0) dd3,
        |if((abs(summary.tiantian_nav - summary.report_nav) <= 0.01  ), 1,0) dd4,
        |if((abs(summary.tiantian_nav - summary.report_nav) <= 0.02  ), 1,0) dd5,
        |if((abs(summary.tiantian_nav - summary.report_nav) >  0.02  ), 1,0) dd6
        |from summary) t17
        |
        |union all
        |select
        |
        |'聚源'          as `基金名称`,
        |concat(cast(sum(bb1) / count(*) * 100 as decimal(16,3)),'%') as `<=0.001`,
        |concat(cast(sum(bb2) / count(*) * 100 as decimal(16,3)),'%') as `<=0.003`,
        |concat(cast(sum(bb3) / count(*) * 100 as decimal(16,3)),'%') as `<=0.005`,
        |concat(cast(sum(bb4) / count(*) * 100 as decimal(16,3)),'%') as `<=0.01 `,
        |concat(cast(sum(bb5) / count(*) * 100 as decimal(16,3)),'%') as `<=0.02 `,
        |concat(cast(sum(bb6) / count(*) * 100 as decimal(16,3)),'%') as `>0.02 `
        |from(
        |select
        |summary.fund_code ,
        |if((abs(summary.jy_nav - summary.report_nav) <= 0.001 ), 1,0) bb1,
        |if((abs(summary.jy_nav - summary.report_nav) <= 0.003 ), 1,0) bb2,
        |if((abs(summary.jy_nav - summary.report_nav) <= 0.005 ), 1,0) bb3,
        |if((abs(summary.jy_nav - summary.report_nav) <= 0.01  ), 1,0) bb4,
        |if((abs(summary.jy_nav - summary.report_nav) <= 0.02  ), 1,0) bb5,
        |if((abs(summary.jy_nav - summary.report_nav) >  0.02  ), 1,0) bb6
        |from summary) t15
        |
        |union all
        |select
        |
        |'好买'         as `基金名称`,
        |concat(cast(sum(cc1) / count(*) * 100 as decimal(16,3)),'%') as `<=0.001`,
        |concat(cast(sum(cc2) / count(*) * 100 as decimal(16,3)),'%') as `<=0.003`,
        |concat(cast(sum(cc3) / count(*) * 100 as decimal(16,3)),'%') as `<=0.005`,
        |concat(cast(sum(cc4) / count(*) * 100 as decimal(16,3)),'%') as `<=0.01 `,
        |concat(cast(sum(cc5) / count(*) * 100 as decimal(16,3)),'%') as `<=0.02 `,
        |concat(cast(sum(cc6) / count(*) * 100 as decimal(16,3)),'%') as `>0.02 `
        |from(
        |select
        |summary.fund_code ,
        |if((abs(summary.Fgz - summary.report_nav) <= 0.001 ), 1,0) cc1,
        |if((abs(summary.Fgz - summary.report_nav) <= 0.003 ), 1,0) cc2,
        |if((abs(summary.Fgz - summary.report_nav) <= 0.005 ), 1,0) cc3,
        |if((abs(summary.Fgz - summary.report_nav) <= 0.01  ), 1,0) cc4,
        |if((abs(summary.Fgz - summary.report_nav) <= 0.02  ), 1,0) cc5,
        |if((abs(summary.Fgz - summary.report_nav) >  0.02  ), 1,0) cc6
        |from summary) t16
        |
        |
        |union all
        |select
        |'万德'         as `基金名称`,
        |concat(cast(sum(zz1) / count(*) * 100 as decimal(16,3)),'%') as `<=0.001`,
        |concat(cast(sum(zz2) / count(*) * 100 as decimal(16,3)),'%') as `<=0.003`,
        |concat(cast(sum(zz3) / count(*) * 100 as decimal(16,3)),'%') as `<=0.005`,
        |concat(cast(sum(zz4) / count(*) * 100 as decimal(16,3)),'%') as `<=0.01 `,
        |concat(cast(sum(zz5) / count(*) * 100 as decimal(16,3)),'%') as `<=0.02 `,
        |concat(cast(sum(zz6) / count(*) * 100 as decimal(16,3)),'%') as `>0.02 `
        |from(
        |select
        |summary.fund_code ,
        |if((abs(summary.WDestimate_return - summary.report_nav) <= 0.001 ), 1,0) zz1,
        |if((abs(summary.WDestimate_return - summary.report_nav) <= 0.003 ), 1,0) zz2,
        |if((abs(summary.WDestimate_return - summary.report_nav) <= 0.005 ), 1,0) zz3,
        |if((abs(summary.WDestimate_return - summary.report_nav) <= 0.01  ), 1,0) zz4,
        |if((abs(summary.WDestimate_return - summary.report_nav) <= 0.02  ), 1,0) zz5,
        |if((abs(summary.WDestimate_return - summary.report_nav) >  0.02  ), 1,0) zz6
        |from summary) t155
        |
        |""".stripMargin)
      /*.coalesce(1)
      .write.mode("Append")
      .option("header", "true")
      .format("CSV")
      .save("D:\\Result\\Main\\Week_NoGrouping\\need4_Absolute_Deviation2")*/.show()


    /**
     *
     * 5. 估值相对偏差的分布
     *   a. 统计估值相对偏差落在以下区间内的个数及占比（该区间的个数/该日总基金的个数）
     *      t18-t21  wind t156 y
     *
     * <=0.001	（0.001,0.003]	(0.003,0.005]	(0.005,0.01]	(0.01,0.02]	>0.02
     */

    spark.sql(
      """
        |
        |select
        |
        |'蚂蚁'  as `基金名称`,
        |concat(cast(sum(e1) / count(fund_code) *100 as decimal(16,3)),'%') as `<=0.05%`,
        |concat(cast(sum(e2) / count(fund_code) *100 as decimal(16,3)),'%') as `(0.05%,0.1%]`,
        |concat(cast(sum(e3) / count(fund_code) *100 as decimal(16,3)),'%') as `(0.1%,0.3]`,
        |concat(cast(sum(e4) / count(fund_code) *100 as decimal(16,3)),'%') as `(0.3%,0.5%]`,
        |concat(cast(sum(e5) / count(fund_code) *100 as decimal(16,3)),'%') as `(0.5%,1%]`,
        |concat(cast(sum(e6) / count(fund_code) *100 as decimal(16,3)),'%') as `>1%`
        |from(
        |select
        |summary.fund_code,
        |if((abs((summary.estimate_return - summary.report_nav) / summary.report_nav)   <=0.0005),1,0) e1 ,
        |if((abs((summary.estimate_return - summary.report_nav) / summary.report_nav)  between 0.0005 and 0.001 ),1,0) e2,
        |if((abs((summary.estimate_return - summary.report_nav) / summary.report_nav)  between 0.001  and 0.003 ),1,0) e3,
        |if((abs((summary.estimate_return - summary.report_nav) / summary.report_nav)  between 0.003  and 0.005 ),1,0) e4,
        |if((abs((summary.estimate_return - summary.report_nav) / summary.report_nav)  between 0.005  and 0.01  ),1,0) e5,
        |if((abs((summary.estimate_return - summary.report_nav) / summary.report_nav)   >0.01),1,0) e6
        |from summary) t18
        |
        |union all
        |select
        |'天天'           as `基金名称`,
        |concat(cast(sum(h1) / count(fund_code) *100 as decimal(16,3)),'%') as `<=0.05%`,
        |concat(cast(sum(h2) / count(fund_code) *100 as decimal(16,3)),'%') as `(0.05%,0.1%]`,
        |concat(cast(sum(h3) / count(fund_code) *100 as decimal(16,3)),'%') as `(0.1%,0.3]`,
        |concat(cast(sum(h4) / count(fund_code) *100 as decimal(16,3)),'%') as `(0.3%,0.5%]`,
        |concat(cast(sum(h5) / count(fund_code) *100 as decimal(16,3)),'%') as `(0.5%,1%]`,
        |concat(cast(sum(h6) / count(fund_code) *100 as decimal(16,3)),'%') as `>1%`
        |from(
        |select
        |summary.fund_code,
        |if((abs(summary.tiantian_nav - summary.report_nav) / summary.report_nav    <=0.0005),1,0) h1 ,
        |if((abs(summary.tiantian_nav - summary.report_nav) / summary.report_nav   between 0.0005 and 0.001 ),1,0) h2,
        |if((abs(summary.tiantian_nav - summary.report_nav) / summary.report_nav   between 0.001  and 0.003 ),1,0) h3,
        |if((abs(summary.tiantian_nav - summary.report_nav) / summary.report_nav   between 0.003  and 0.005 ),1,0) h4,
        |if((abs(summary.tiantian_nav - summary.report_nav) / summary.report_nav   between 0.005  and 0.01  ),1,0) h5,
        |if((abs(summary.tiantian_nav - summary.report_nav) / summary.report_nav    >0.01),1,0) h6
        |from summary) t21
        |
        |union all
        |select
        |
        |'聚源'            as `基金名称`,
        |concat(cast(sum(f1) / count(fund_code) *100 as decimal(16,3)),'%') as `<=0.05%`,
        |concat(cast(sum(f2) / count(fund_code) *100 as decimal(16,3)),'%') as `(0.05%,0.1%]`,
        |concat(cast(sum(f3) / count(fund_code) *100 as decimal(16,3)),'%') as `(0.1%,0.3]`,
        |concat(cast(sum(f4) / count(fund_code) *100 as decimal(16,3)),'%') as `(0.3%,0.5%]`,
        |concat(cast(sum(f5) / count(fund_code) *100 as decimal(16,3)),'%') as `(0.5%,1%]`,
        |concat(cast(sum(f6) / count(fund_code) *100 as decimal(16,3)),'%') as `>1%`
        |from(
        |select
        |summary.fund_code,
        |if((abs(summary.jy_nav - summary.report_nav) / summary.report_nav    <=0.0005),1,0) f1 ,
        |if((abs(summary.jy_nav - summary.report_nav) / summary.report_nav   between 0.0005 and 0.001 ),1,0) f2,
        |if((abs(summary.jy_nav - summary.report_nav) / summary.report_nav   between 0.001  and 0.003 ),1,0) f3,
        |if((abs(summary.jy_nav - summary.report_nav) / summary.report_nav   between 0.003  and 0.005 ),1,0) f4,
        |if((abs(summary.jy_nav - summary.report_nav) / summary.report_nav   between 0.005  and 0.01  ),1,0) f5,
        |if((abs(summary.jy_nav - summary.report_nav) / summary.report_nav    >0.01),1,0) f6
        |from summary) t19
        |
        |union all
        |select
        |
        |'好买'           as `基金名称`,
        |concat(cast(sum(g1) / count(fund_code) *100 as decimal(16,3)),'%') as `<=0.05%`,
        |concat(cast(sum(g2) / count(fund_code) *100 as decimal(16,3)),'%') as `(0.05%,0.1%]`,
        |concat(cast(sum(g3) / count(fund_code) *100 as decimal(16,3)),'%') as `(0.1%,0.3]`,
        |concat(cast(sum(g4) / count(fund_code) *100 as decimal(16,3)),'%') as `(0.3%,0.5%]`,
        |concat(cast(sum(g5) / count(fund_code) *100 as decimal(16,3)),'%') as `(0.5%,1%]`,
        |concat(cast(sum(g6) / count(fund_code) *100 as decimal(16,3)),'%') as `>1%`
        |from(
        |select
        |summary.fund_code,
        |if((abs(summary.Fgz - summary.report_nav) / summary.report_nav    <=0.0005),1,0) g1 ,
        |if((abs(summary.Fgz - summary.report_nav) / summary.report_nav   between 0.0005 and 0.001 ),1,0) g2,
        |if((abs(summary.Fgz - summary.report_nav) / summary.report_nav   between 0.001  and 0.003 ),1,0) g3,
        |if((abs(summary.Fgz - summary.report_nav) / summary.report_nav   between 0.003  and 0.005 ),1,0) g4,
        |if((abs(summary.Fgz - summary.report_nav) / summary.report_nav   between 0.005  and 0.01  ),1,0) g5,
        |if((abs(summary.Fgz - summary.report_nav) / summary.report_nav    >0.01),1,0) g6
        |from summary) t20
        |
        |
        |union all
        |select
        |'万德'           as `基金名称`,
        |concat(cast(sum(y1) / count(fund_code) *100 as decimal(16,3)),'%') as `<=0.05%`,
        |concat(cast(sum(y2) / count(fund_code) *100 as decimal(16,3)),'%') as `(0.05%,0.1%]`,
        |concat(cast(sum(y3) / count(fund_code) *100 as decimal(16,3)),'%') as `(0.1%,0.3]`,
        |concat(cast(sum(y4) / count(fund_code) *100 as decimal(16,3)),'%') as `(0.3%,0.5%]`,
        |concat(cast(sum(y5) / count(fund_code) *100 as decimal(16,3)),'%') as `(0.5%,1%]`,
        |concat(cast(sum(y6) / count(fund_code) *100 as decimal(16,3)),'%') as `>1%`
        |from(
        |select
        |summary.fund_code,
        |if((abs(summary.WDestimate_return - summary.report_nav) / summary.report_nav    <=0.0005),1,0) y1 ,
        |if((abs(summary.WDestimate_return - summary.report_nav) / summary.report_nav   between 0.0005 and 0.001 ),1,0) y2,
        |if((abs(summary.WDestimate_return - summary.report_nav) / summary.report_nav   between 0.001  and 0.003 ),1,0) y3,
        |if((abs(summary.WDestimate_return - summary.report_nav) / summary.report_nav   between 0.003  and 0.005 ),1,0) y4,
        |if((abs(summary.WDestimate_return - summary.report_nav) / summary.report_nav   between 0.005  and 0.01  ),1,0) y5,
        |if((abs(summary.WDestimate_return - summary.report_nav) / summary.report_nav    >0.01),1,0) y6
        |from summary) t156
        |
        |""".stripMargin)
      /*.coalesce(1)
      .write.mode("Append")
      .option("header", "true")
      .format("CSV")
      .save("D:\\Result\\Main\\Week_NoGrouping\\need5_Relative_Deviation1")*/.show()





    /**
     *
     * b. 统计估值绝对偏差落在以下区间内的个数及占比（该区间的个数/该日总基金的个数） t22-t25 wind t157 yy
     *
     * <=0.05%	<=0.1%	<=0.3%	<=0.5%	<=1%	>1%    完成的
     */
    spark.sql(
      """
        |
        |select
        |'蚂蚁'           as `基金名称`,
        |concat(cast(sum(ee1) / count(*) *100 as decimal(16,3)),'%') as `<=0.05%`,
        |concat(cast(sum(ee2) / count(*) *100 as decimal(16,3)),'%') as `<=0.1% `,
        |concat(cast(sum(ee3) / count(*) *100 as decimal(16,3)),'%') as `<=0.3% `,
        |concat(cast(sum(ee4) / count(*) *100 as decimal(16,3)),'%') as `<=0.5% `,
        |concat(cast(sum(ee5) / count(*) *100 as decimal(16,3)),'%') as `<=1%  ` ,
        |concat(cast(sum(ee6) / count(*) *100 as decimal(16,3)),'%') as ` >1%   `
        |from
        |(
        |select
        |if((abs(summary.estimate_return - summary.report_nav) / summary.report_nav   <= 0.0005),1,0) ee1,
        |if((abs(summary.estimate_return - summary.report_nav) / summary.report_nav   <= 0.001 ),1,0) ee2,
        |if((abs(summary.estimate_return - summary.report_nav) / summary.report_nav   <= 0.003 ),1,0) ee3,
        |if((abs(summary.estimate_return - summary.report_nav) / summary.report_nav   <= 0.005 ),1,0) ee4,
        |if((abs(summary.estimate_return - summary.report_nav) / summary.report_nav   <= 0.01  ),1,0) ee5,
        |if((abs(summary.estimate_return - summary.report_nav) / summary.report_nav   >  0.01  ),1,0) ee6
        |from summary) t22
        |
        |union all
        |select
        |'天天'          as `基金名称`,
        |concat(cast(sum(hh1) / count(*) *100 as decimal(16,3)),'%') as `<=0.05%`,
        |concat(cast(sum(hh2) / count(*) *100 as decimal(16,3)),'%') as `<=0.1% `,
        |concat(cast(sum(hh3) / count(*) *100 as decimal(16,3)),'%') as `<=0.3% `,
        |concat(cast(sum(hh4) / count(*) *100 as decimal(16,3)),'%') as `<=0.5% `,
        |concat(cast(sum(hh5) / count(*) *100 as decimal(16,3)),'%') as `<=1%  ` ,
        |concat(cast(sum(hh6) / count(*) *100 as decimal(16,3)),'%') as ` >1%   `
        |from(
        |select
        |if((abs(summary.tiantian_nav - summary.report_nav) / summary.report_nav   <= 0.0005),1,0) hh1,
        |if((abs(summary.tiantian_nav - summary.report_nav) / summary.report_nav   <= 0.001 ),1,0) hh2,
        |if((abs(summary.tiantian_nav - summary.report_nav) / summary.report_nav   <= 0.003 ),1,0) hh3,
        |if((abs(summary.tiantian_nav - summary.report_nav) / summary.report_nav   <= 0.005 ),1,0) hh4,
        |if((abs(summary.tiantian_nav - summary.report_nav) / summary.report_nav   <= 0.01  ),1,0) hh5,
        |if((abs(summary.tiantian_nav - summary.report_nav) / summary.report_nav   >  0.01  ),1,0) hh6
        |from summary) t25
        |
        |union all
        |select
        |'聚源'          as `基金名称`,
        |concat(cast(sum(ff1) / count(*) *100 as decimal(16,3)),'%') as `<=0.05%`,
        |concat(cast(sum(ff2) / count(*) *100 as decimal(16,3)),'%') as `<=0.1% `,
        |concat(cast(sum(ff3) / count(*) *100 as decimal(16,3)),'%') as `<=0.3% `,
        |concat(cast(sum(ff4) / count(*) *100 as decimal(16,3)),'%') as `<=0.5% `,
        |concat(cast(sum(ff5) / count(*) *100 as decimal(16,3)),'%') as `<=1%  ` ,
        |concat(cast(sum(ff6) / count(*) *100 as decimal(16,3)),'%') as ` >1%   `
        |from(
        |select
        |if((abs(summary.jy_nav - summary.report_nav) / summary.report_nav   <= 0.0005),1,0) ff1,
        |if((abs(summary.jy_nav - summary.report_nav) / summary.report_nav   <= 0.001 ),1,0) ff2,
        |if((abs(summary.jy_nav - summary.report_nav) / summary.report_nav   <= 0.003 ),1,0) ff3,
        |if((abs(summary.jy_nav - summary.report_nav) / summary.report_nav   <= 0.005 ),1,0) ff4,
        |if((abs(summary.jy_nav - summary.report_nav) / summary.report_nav   <= 0.01  ),1,0) ff5,
        |if((abs(summary.jy_nav - summary.report_nav) / summary.report_nav   >  0.01  ),1,0) ff6
        |from summary) t23
        |
        |union all
        |select
        |'好买'          as `基金名称`,
        |concat(cast(sum(gg1) / count(*) *100 as decimal(16,3)),'%') as `<=0.05%`,
        |concat(cast(sum(gg2) / count(*) *100 as decimal(16,3)),'%') as `<=0.1% `,
        |concat(cast(sum(gg3) / count(*) *100 as decimal(16,3)),'%') as `<=0.3% `,
        |concat(cast(sum(gg4) / count(*) *100 as decimal(16,3)),'%') as `<=0.5% `,
        |concat(cast(sum(gg5) / count(*) *100 as decimal(16,3)),'%') as `<=1%  ` ,
        |concat(cast(sum(gg6) / count(*) *100 as decimal(16,3)),'%') as ` >1%   `
        |from(
        |select
        |if((abs(summary.Fgz - summary.report_nav) / summary.report_nav   <= 0.0005),1,0) gg1,
        |if((abs(summary.Fgz - summary.report_nav) / summary.report_nav   <= 0.001 ),1,0) gg2,
        |if((abs(summary.Fgz - summary.report_nav) / summary.report_nav   <= 0.003 ),1,0) gg3,
        |if((abs(summary.Fgz - summary.report_nav) / summary.report_nav   <= 0.005 ),1,0) gg4,
        |if((abs(summary.Fgz - summary.report_nav) / summary.report_nav   <= 0.01  ),1,0) gg5,
        |if((abs(summary.Fgz - summary.report_nav) / summary.report_nav   >  0.01  ),1,0) gg6
        |from summary) t24
        |
        |union all
        |select
        |'万德'          as `基金名称`,
        |concat(cast(sum(zz1) / count(*) *100 as decimal(16,3)),'%') as `<=0.05%`,
        |concat(cast(sum(zz2) / count(*) *100 as decimal(16,3)),'%') as `<=0.1% `,
        |concat(cast(sum(zz3) / count(*) *100 as decimal(16,3)),'%') as `<=0.3% `,
        |concat(cast(sum(zz4) / count(*) *100 as decimal(16,3)),'%') as `<=0.5% `,
        |concat(cast(sum(zz5) / count(*) *100 as decimal(16,3)),'%') as `<=1%  ` ,
        |concat(cast(sum(zz6) / count(*) *100 as decimal(16,3)),'%') as ` >1%   `
        |from(
        |select
        |if((abs(summary.WDestimate_return - summary.report_nav) / summary.report_nav   <= 0.0005),1,0) zz1,
        |if((abs(summary.WDestimate_return - summary.report_nav) / summary.report_nav   <= 0.001 ),1,0) zz2,
        |if((abs(summary.WDestimate_return - summary.report_nav) / summary.report_nav   <= 0.003 ),1,0) zz3,
        |if((abs(summary.WDestimate_return - summary.report_nav) / summary.report_nav   <= 0.005 ),1,0) zz4,
        |if((abs(summary.WDestimate_return - summary.report_nav) / summary.report_nav   <= 0.01  ),1,0) zz5,
        |if((abs(summary.WDestimate_return - summary.report_nav) / summary.report_nav   >  0.01  ),1,0) zz6
        |from summary) t157
        |
        |""".stripMargin)
     /* .coalesce(1)
      .write.mode("Append")
      .option("header", "true")
      .format("CSV")
      .save("D:\\Result\\Main\\Week_NoGrouping\\need5_Absolute_Deviation2")*/.show()


    spark.stop()


  }

}

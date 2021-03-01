package Bean

import java.sql.Date

case class WeekAll(
                    fund_code: String,       //基金代码
                    Type: String,            //基金类型
                    time_int: Date,          //时间
                    report_nav: Float,       //披露净值
                    report_return: Float,    //披露收益率
                    estimate_return: Float,  //蚂蚁估算净值
                    estimate_ratio: Float,   //蚂蚁估算收益率
                    Fgz: Float,              //好买估算净值
                    Fzdbl: Float,            //好买估算收益率
                    jy_nav: Float,           //聚源估算净值
                    jy_return: Float,        //聚源估算收益率
                    tiantian_nav: Float,     //天天估算净值
                    tiantian_return: Float,  //天天估算收益率
                    WDestimate_return: Float //万德估算净值


                  )

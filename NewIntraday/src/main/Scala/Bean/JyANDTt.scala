package Bean

import java.sql.Date

case class JyANDTt(
                fund_code: String, //基金代码
                fund_type: String, //类型
                time_int: Date, //时间
                jy_nav: Float, //聚源估净值
                jy_return: Float, //聚源估算收益率
                report_nav: Float, //披露净值
                report_return: Float, //披露收益率
                jy_sign_error: String, //聚源涨跌相反标识
                jy_deviation: String, //聚源估算偏差
                tiantian_nav: Float, //天天估算净值
                tiantian_return: Float, //天天估算收益率
                tiantian_sign_error: String, //天天涨跌相反标识
                tiantian_deviation: String //天天估算偏差
              )
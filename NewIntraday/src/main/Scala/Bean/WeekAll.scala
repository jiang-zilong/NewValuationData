package Bean

import java.sql.Date

case class WeekAll(
                    fund_code: String,
                    Type: String,
                    time_int: Date,
                    report_nav: Float,
                    report_return: Float,
                    estimate_return: Float,
                    estimate_ratio: Float,
                    Fgz: Float,
                    Fzdbl: Float,
                    jy_nav: Float,
                    jy_return: Float,
                    tiantian_nav: Float,
                    tiantian_return: Float


                  )

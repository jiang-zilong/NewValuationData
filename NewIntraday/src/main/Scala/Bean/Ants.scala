package Bean

import java.sql.Date


case class Ants(
                 fproduct_code: String,
                 fproduct_name: String,
                 ftrading_day: String,
                 ftotal_return: String,
                 download_date: Date,
                 estimate_date: String,
                 estimate_return: Float, // 蚂蚁估算净值
                 estimate_ratio: String //  蚂蚁估算收益率  "+ %" 形式 原封不动,计算的时候要注意
               )

package Bean

import java.sql.Date

/**
 * @author v_ziljiang
 * @date 2021/3/4 16:27
 */
case class Fund_top(
                     date: String,              //日期
                     FA: String,                //资产分类
                     code: String,              //基金商户号
                     fund_code: String,         //基金代码
                     fund_name: String,         //基金简称
                     Number_users_held: String, //持有用户数
                     M: String                  //保有量(元)
                   )



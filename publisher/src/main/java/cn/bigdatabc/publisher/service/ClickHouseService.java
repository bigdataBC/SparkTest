package cn.bigdatabc.publisher.service;

import java.math.BigDecimal;
import java.util.Map;

/**
 * Desc: ClickHourse相关相关的业务接口
 */
public interface ClickHouseService {
    //获取指定日期总的交易额
    BigDecimal getOrderAmountTocal(String date);

    //获取指定日期的分时交易额

    /*
     * 从Mapper获取的分时交易额格式  List<Map{hr->11,am->10000}> ==>Map{11->10000,12->2000}
     * @param date
     * @return
     */
    Map<String,BigDecimal> getOrderAmountHour(String date);
}

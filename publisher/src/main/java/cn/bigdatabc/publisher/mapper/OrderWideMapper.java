package cn.bigdatabc.publisher.mapper;

import org.apache.ibatis.annotations.Mapper;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

/**
 * Desc: 对订单宽表进行操作的接口
 */
@Mapper
public interface OrderWideMapper {
    /**
     * 获取指定日期的交易额
     *
     * @param date
     * @return
     */
    BigDecimal selectOrderAmountTotal(String date);

    /**
     * 获取指定日期的分时交易额
     *
     * @param date
     * @return
     */
    List<Map> selectOrderAmountHour(String date);
}

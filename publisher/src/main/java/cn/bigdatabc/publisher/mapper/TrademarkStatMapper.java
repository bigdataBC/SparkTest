package cn.bigdatabc.publisher.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;
import java.util.Map;

@Mapper
public interface TrademarkStatMapper {
    /**
     * 计算某个时间段的热门品牌 TopN
     *
     * @param startTime
     * @param endTime
     * @param topN
     * @return
     */
    List<Map> selectTradeSum(@Param("start_time") String startTime,
                             @Param("end_time") String endTime,
                             @Param("topN") int topN);
}

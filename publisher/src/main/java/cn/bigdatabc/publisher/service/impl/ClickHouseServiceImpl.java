package cn.bigdatabc.publisher.service.impl;

import cn.bigdatabc.publisher.mapper.OrderWideMapper;
import cn.bigdatabc.publisher.service.ClickHouseService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class ClickHouseServiceImpl implements ClickHouseService {

    @Autowired
    OrderWideMapper orderWideMapper;

    @Override
    public BigDecimal getOrderAmountTocal(String date) {
        return orderWideMapper.selectOrderAmountTotal(date);
    }

    @Override  //List<Map{hr->11,am->10000}>  ==> Map<String, BigDecimal>
    public Map<String, BigDecimal> getOrderAmountHour(String date) {
        Map<String, BigDecimal> rsMap = new HashMap<String, BigDecimal>();
        List<Map> mapList = orderWideMapper.selectOrderAmountHour(date);
        for (Map map : mapList) {
            //注意：key的名称不能随便写，和mapper映射文件中，查询语句的别名一致
            rsMap.put(String.format("%02d",map.get("hr")),(BigDecimal) map.get("am"));
        }
        return rsMap;
    }
}

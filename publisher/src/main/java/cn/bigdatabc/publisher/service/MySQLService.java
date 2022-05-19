package cn.bigdatabc.publisher.service;

import java.util.List;
import java.util.Map;

public interface MySQLService {
    List<Map> getTradeAmount(String startTime, String endTime, int topN);
}

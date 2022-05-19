package cn.bigdatabc.publisher.controller;

import cn.bigdatabc.publisher.service.MySQLService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
public class DataVController {

    @Autowired
    MySQLService mySQLService;

    @RequestMapping("/trademark-sum")
    public Object trademarkSum(@RequestParam("start_date")String startTime, @RequestParam("end_date")String endTime, @RequestParam("topN")int topN){
        List<Map> rsMap = mySQLService.getTradeAmount(startTime, endTime, topN);
        return rsMap;
    }
}

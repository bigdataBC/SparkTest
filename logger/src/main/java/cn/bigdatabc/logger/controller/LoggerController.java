package cn.bigdatabc.logger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @description：日志接收落盘并生产至Kafka
 * @author     ：bboy枫亭
 * @date       ：Created in 2022/5/9 18:34
 */
@RestController
@Slf4j
public class LoggerController {
    /**
     * 将KafkaTemplate注入到Controller中
     */
    @Autowired
    KafkaTemplate kafkaTemplate;

    /**
     * 提供一个方法，处理模拟器生成的数据
     * http://localhost:8989/applog
     *
     * @RequestMapping("/applog")  把applog请求，交给方法进行处理
     * @RequestBody   表示从请求体中获取数据
     *
     * @param mockLog
     * @return
     */
    @RequestMapping("/applog")
    public String applog(@RequestBody String mockLog) {
        // 1、落盘
        log.info(mockLog);

        // 2、生产至Kafka
        //  2.1、将接收到的字符串数据转换为json对象
        JSONObject jsonObject = JSON.parseObject(mockLog);
        JSONObject startJSON = jsonObject.getJSONObject("start");
        //  2.2、根据日志的类型，发送到kafka的不同主题中去
        if (startJSON != null) {
            kafkaTemplate.send("gmall_start_0523", mockLog);
        } else {
            kafkaTemplate.send("gmall_event_0523", mockLog);
        }

        return "[SUCCESS]:message received and processed";
    }
}

package com.lwl.kafka.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/kafka")
public class KafkaController {
    protected final Logger logger = LoggerFactory.getLogger(this.getClass());
    @Autowired
    private KafkaTemplate kafkaTemplate;

    //自动提交
    @RequestMapping("/send")
    public String send(@RequestParam(required = false) String key, @RequestParam(required = false) String value) {
        try {
            logger.info("kafka的消息：{}", value);
            kafkaTemplate.send("test", key, value);
            return "发送成功！";
        } catch (Exception e) {
            logger.info("发送异常：{}", e);
            return "发送失败！";
        }
    }

    //手动提交
    @RequestMapping("/sendAck")
    public String sendAck(@RequestParam(required = false) String key, @RequestParam(required = false) String value) {
        try {
            logger.info("kafka的消息：{}", value);
            kafkaTemplate.send("testAck", key, value);
            return "发送成功！";
        } catch (Exception e) {
            logger.info("发送异常：{}", e);
            return "发送失败！";
        }
    }
}

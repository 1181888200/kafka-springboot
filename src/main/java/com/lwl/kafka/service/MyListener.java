package com.lwl.kafka.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
/**
 * 自动提交监听器
 */
public class MyListener {

    protected final Logger logger = LoggerFactory.getLogger(MyListener.class);

    @KafkaListener(topics = {"test"}, containerFactory = "factory")
    public void listener(ConsumerRecord<?, ?> record) {
        logger.info("收到消息的key：" + record.key());
        logger.info("收到消息的value：" + record.value());
    }
}

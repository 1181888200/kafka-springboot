package com.lwl.kafka.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;

/**
 * @description： 手动提交监听器
 *                  需要用户自己确认消息，如果一直不确认，则消息一直存在
 *                  可以把28行注释掉，然后推送一条数据，发现消费者是可以消费的，但是没有确认，关闭服务再重启之后，发现消费者又消费了一遍，但是还是没有确认，需要手动提交确认
 * @author     ：lwl
 * @date       ：2020/8/3 14:38
 * @version:     1.0.0
 */
public class MyListenerAck {

    protected final Logger logger = LoggerFactory.getLogger(MyListenerAck.class);

    @KafkaListener(topics = {"testAck"}, containerFactory = "factoryAck")
    public void listener(ConsumerRecord<?, ?> record, Acknowledgment acknowledgment) {
        try {
            logger.info("手动确认收到消息的key：" + record.key());
            logger.info("手动确认收到消息的value：" + record.value());
        } finally {
            logger.info("消息确认！");
            acknowledgment.acknowledge();
        }
    }
}

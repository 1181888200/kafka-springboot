package com.lwl.kafka;


import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;



@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest
@WebAppConfiguration
public class SendKafkaTest {
    protected final Logger logger = LoggerFactory.getLogger(this.getClass());


    @Autowired
    private KafkaTemplate kafkaTemplate;


    @Test
    public void sendTest() {
        String value ="我是大帅哥";
        String key = "top.one";
        try {
            logger.info("kafka的消息：{}", value);
            kafkaTemplate.send("test", key, value);
            System.out.println("发送成功！");
        } catch (Exception e) {
            logger.info("发送异常：{}", e);
            System.out.println("发送失败！");
        }
    }

    //手动提交
    @Test
    public void sendAckTest() {
        String value ="我是Ack22222222";
        String key = "top.one";
        try {
            logger.info("kafka的消息：{}", value);
            kafkaTemplate.send("testAck", key, value);
            System.out.println("发送成功！");
        } catch (Exception e) {
            logger.info("发送异常：{}", e);
            System.out.println("发送失败！");
        }
    }
}

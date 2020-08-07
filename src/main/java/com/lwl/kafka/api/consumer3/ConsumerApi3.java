package com.lwl.kafka.api.consumer3;

import com.lwl.kafka.api.consumer.ConsumerApi;
import com.lwl.kafka.api.consumer2.ConsumerApi2;
import com.lwl.kafka.api.producer.ProducerApi;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * @description： 多个分区，一个消费者，采用多线程处理数据
 *          这种模式虽然加快了数据的处理，但是当获取到数据的时候，交由多线程处理之后，就不能控制他的偏移量提交，如果数据不是要求一致性很强的话，可以考虑
 * @author     ：lwl
 * @date       ：2020/8/7 14:29
 * @version:     1.0.0
 */
public class ConsumerApi3 {


    public static void main(String[] args) throws Exception {


        // 构建数据--->发送消息
//        ProducerApi.sendMessageToTopic(ConsumerApi2.TOPIC_NAME,20);


        /**
         *
         *  由5个线程同时处理数据
         *  thread = pool-1-thread-2 topic = multi.consumer.topic partition = 1 offset = 40, key = key-4, value = value-4
         *  thread = pool-1-thread-5 topic = multi.consumer.topic partition = 1 offset = 43, key = key-9, value = value-9
         *  thread = pool-1-thread-4 topic = multi.consumer.topic partition = 1 offset = 42, key = key-8, value = value-8
         *  thread = pool-1-thread-3 topic = multi.consumer.topic partition = 1 offset = 41, key = key-7, value = value-7
         *  thread = pool-1-thread-1 topic = multi.consumer.topic partition = 1 offset = 39, key = key-3, value = value-3
         *  thread = pool-1-thread-1 topic = multi.consumer.topic partition = 1 offset = 48, key = key-18, value = value-18
         *  thread = pool-1-thread-3 topic = multi.consumer.topic partition = 1 offset = 47, key = key-16, value = value-16
         *  thread = pool-1-thread-4 topic = multi.consumer.topic partition = 1 offset = 46, key = key-15, value = value-15
         *  thread = pool-1-thread-5 topic = multi.consumer.topic partition = 1 offset = 45, key = key-13, value = value-13
         *  thread = pool-1-thread-2 topic = multi.consumer.topic partition = 1 offset = 44, key = key-11, value = value-11
         *  thread = pool-1-thread-5 topic = multi.consumer.topic partition = 0 offset = 35, key = key-6, value = value-6
         *  thread = pool-1-thread-4 topic = multi.consumer.topic partition = 0 offset = 34, key = key-5, value = value-5
         *  thread = pool-1-thread-3 topic = multi.consumer.topic partition = 0 offset = 33, key = key-2, value = value-2
         *  thread = pool-1-thread-1 topic = multi.consumer.topic partition = 0 offset = 32, key = key-1, value = value-1
         *  thread = pool-1-thread-3 topic = multi.consumer.topic partition = 0 offset = 39, key = key-17, value = value-17
         *  thread = pool-1-thread-4 topic = multi.consumer.topic partition = 0 offset = 38, key = key-14, value = value-14
         *  thread = pool-1-thread-5 topic = multi.consumer.topic partition = 0 offset = 37, key = key-12, value = value-12
         *  thread = pool-1-thread-2 topic = multi.consumer.topic partition = 0 offset = 36, key = key-10, value = value-10
         *  thread = pool-1-thread-1 topic = multi.consumer.topic partition = 0 offset = 40, key = key-19, value = value-19
         *
         */
        // 测试
        KafkaConsumer c1 = getKafkaConsumer();
        c1.subscribe(Arrays.asList(ConsumerApi2.TOPIC_NAME));
        KafkaConsumerExcuter excuter = new KafkaConsumerExcuter(c1);
        // 开始执行
        excuter.execute(5);

    }

    public static KafkaConsumer getKafkaConsumer() {
        Properties props = ConsumerApi.getCommonPros();
        // 消费者对象
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(props);
        return consumer;
    }

}

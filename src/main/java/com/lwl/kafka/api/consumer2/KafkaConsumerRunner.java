package com.lwl.kafka.api.consumer2;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @description： 消费者线程，如果是多个消费者的话，一般是一个消费者会消费一个分区
 *
 *      一个分区只能由一个消费者消费
 *      一个消费者能消费多个分区
 *
 * @author     ：lwl
 * @date       ：2020/8/7 13:55
 * @version:     1.0.0
 */
public class KafkaConsumerRunner implements Runnable{

    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final KafkaConsumer consumer;

    public KafkaConsumerRunner(KafkaConsumer consumer) {
        this.consumer = consumer;
    }


    @Override
    public void run() {
        try {
            consumer.subscribe(Arrays.asList(ConsumerApi2.TOPIC_NAME));
            while (!closed.get()) {
                // 通过拉的方式，获取数据，此数据是一个一批一批的，也就是一批中会有多个消息
                ConsumerRecords<String, String> records  = consumer.poll(1000);
                // 遍历获取消息
                for (ConsumerRecord<String, String> record: records){
                    System.out.printf(" thread = %s  topic = %s partition = %d offset = %d, key = %s, value = %s%n",
                            Thread.currentThread().getName(),
                            record.topic(), record.partition(), record.offset(), record.key(), record.value());
                }
                // 需要手动提交一下
                // 如果把下面这一行注释掉，那么每次执行的时候，都会从上一次已提交的offset 位置重新读取数据，也就造成了消息的重复消费
                consumer.commitAsync();
            }
        } catch (WakeupException e) {
            // Ignore exception if closing
            if (!closed.get()) {
                throw e;
            }
        } finally {
            consumer.close();
        }
    }

    public void shutdown() {
        closed.set(true);
        consumer.wakeup();
    }
}

package com.lwl.kafka.api.consumer3;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * @description： 处理消息的逻辑
 * @author     ：lwl
 * @date       ：2020/8/7 14:44
 * @version:     1.0.0
 */
public class ConsumerRecordWorker implements Runnable {

    private ConsumerRecord record;

    public ConsumerRecordWorker(ConsumerRecord record){
        this.record = record;
    }

    @Override
    public void run() {
        System.out.printf(" thread = %s topic = %s partition = %d offset = %d, key = %s, value = %s%n",
                Thread.currentThread().getName(), record.topic(), record.partition(), record.offset(), record.key(), record.value());
    }
}

package com.lwl.kafka.api.consumer3;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.concurrent.*;

/**
 * @description： 线程池
 * @author     ：lwl
 * @date       ：2020/8/7 14:31
 * @version:     1.0.0
 */
public class KafkaConsumerExcuter {


    private final KafkaConsumer consumer;

    private ExecutorService executors = null;

    public KafkaConsumerExcuter(KafkaConsumer consumer) {
        this.consumer = consumer;
    }



    public void execute(int workerNum){

        executors = new ThreadPoolExecutor(workerNum, workerNum, 0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(1000), new ThreadPoolExecutor.CallerRunsPolicy());

        // 持续从主题中获取消息
        while (true){
            // 通过拉的方式，获取数据，此数据是一个一批一批的，也就是一批中会有多个消息
            ConsumerRecords<String, String> records  = consumer.poll(1000);
            // 遍历获取消息
            for (ConsumerRecord<String, String> record: records){
                // 任务交由线程池处理
                executors.submit(new ConsumerRecordWorker(record));
            }
        }

    }

}

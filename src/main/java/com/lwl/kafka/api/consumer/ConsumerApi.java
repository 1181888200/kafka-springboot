package com.lwl.kafka.api.consumer;

import com.lwl.kafka.api.producer.ProducerApi;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * @description： 消费者
 * @author     ：lwl
 * @date       ：2020/8/7 8:28
 * @version:     1.0.0
 */
public class ConsumerApi {

    private static final String TOPIC_NAME = "consumer.topic";

    public static void main(String[] args) throws Exception {

        // 第一步：往主题发送100条测试数据
//        ProducerApi.sendMessageToTopic(TOPIC_NAME,10);

        // 测试一
        // 自动消费数据
        /**
         *  topic = consumer.topic partition = 0 offset = 0, key = key-1, value = value-1
         *  topic = consumer.topic partition = 0 offset = 1, key = key-2, value = value-2
         *  topic = consumer.topic partition = 0 offset = 2, key = key-3, value = value-3
         *  topic = consumer.topic partition = 0 offset = 3, key = key-4, value = value-4
         *  topic = consumer.topic partition = 0 offset = 4, key = key-5, value = value-5
         *  topic = consumer.topic partition = 0 offset = 5, key = key-6, value = value-6
         *  topic = consumer.topic partition = 0 offset = 6, key = key-7, value = value-7
         *  topic = consumer.topic partition = 0 offset = 7, key = key-8, value = value-8
         *  topic = consumer.topic partition = 0 offset = 8, key = key-9, value = value-9
         */
//        consumerSimple();


        // 测试二
        // 手动提交偏移量offset
        // 如果忘记提交偏移量，则会导致每次消费的消息都是从上一次提交的偏移量开始，导致消息被多次重复消费
        /**
         *  此时的偏移量 offset = 9 是上一批最后的偏移量 + 1
         *  topic = consumer.topic partition = 0 offset = 9, key = key-1, value = value-1
         *  topic = consumer.topic partition = 0 offset = 10, key = key-2, value = value-2
         *  topic = consumer.topic partition = 0 offset = 11, key = key-3, value = value-3
         *  topic = consumer.topic partition = 0 offset = 12, key = key-4, value = value-4
         *  topic = consumer.topic partition = 0 offset = 13, key = key-5, value = value-5
         *  topic = consumer.topic partition = 0 offset = 14, key = key-6, value = value-6
         *  topic = consumer.topic partition = 0 offset = 15, key = key-7, value = value-7
         *  topic = consumer.topic partition = 0 offset = 16, key = key-8, value = value-8
         *  topic = consumer.topic partition = 0 offset = 17, key = key-9, value = value-9
         */
//        consumerCommitOffSet();

    }



    /**
     *  消费者简单模式
     *      只是从主题中消费数据，而且自动提交偏移量offset
     * result:
     * author: lwl
     * date: 2020/8/7 10:26
     */
    public static void consumerSimple(){
        Properties props = getCommonPros();
        // 消费者对象
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(props);

        // 消费者订阅主题
        consumer.subscribe(Arrays.asList(TOPIC_NAME));

        // 持续从主题中获取消息
        while (true){
            // 通过拉的方式，获取数据，此数据是一个一批一批的，也就是一批中会有多个消息
            ConsumerRecords<String, String> records  = consumer.poll(1000);
             // 遍历获取消息
            for (ConsumerRecord<String, String> record: records){
                System.out.printf(" topic = %s partition = %d offset = %d, key = %s, value = %s%n",
                        record.topic(), record.partition(), record.offset(), record.key(), record.value());
            }
        }
    }


    /**
     *  消费者手动提交偏移量
     * result:
     * author: lwl
     * date: 2020/8/7 10:29
     */
    public static void consumerCommitOffSet(){
        Properties props = getCommonPros();
        // 把自动提交设置为false 即可
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        // 消费者对象
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(props);

        // 消费者订阅主题
        consumer.subscribe(Arrays.asList(TOPIC_NAME));

        // 持续从主题中获取消息
        while (true){
            // 通过拉的方式，获取数据，此数据是一个一批一批的，也就是一批中会有多个消息
            ConsumerRecords<String, String> records  = consumer.poll(1000);
            // 遍历获取消息
            for (ConsumerRecord<String, String> record: records){
                System.out.printf(" topic = %s partition = %d offset = %d, key = %s, value = %s%n",
                        record.topic(), record.partition(), record.offset(), record.key(), record.value());
            }

            // 需要手动提交一下
            // 如果把下面这一行注释掉，那么每次执行的时候，都会从上一次已提交的offset 位置重新读取数据，也就造成了消息的重复消费
            consumer.commitAsync();
        }
    }



    /**
     *  获取消费者基础配置
     * result:
     * author: lwl
     * date: 2020/8/7 10:21
     */
    public static Properties getCommonPros(){
        Properties props = new Properties();
        // kafka集群地址，多个用逗号隔开
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.31.28:9092");
        // 消费者群组，后期会重点说道
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
        // 自动提交offset
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        // 自动提交offset间隔时间
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");

        // 键值序列化方式
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        return props;
    }

}

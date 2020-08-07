package com.lwl.kafka.api.consumer2;

import com.lwl.kafka.api.admin.AdminApi;
import com.lwl.kafka.api.consumer.ConsumerApi;
import com.lwl.kafka.api.producer.ProducerApi;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Properties;

/**
 * @description： 多消费者模式
 *                  一个消费者 对应一个分区，这种情况是比较好的，由于偏移量的提交和问题的处理，也有利于分区的管理
 * @author     ：lwl
 * @date       ：2020/8/7 13:58
 * @version:     1.0.0
 */
public class ConsumerApi2 {

    public static final String TOPIC_NAME = "multi.consumer.topic";


    public static void main(String[] args) throws Exception {

        // 构建数据--->发送消息
//        ProducerApi.sendMessageToTopic(TOPIC_NAME,12);


        // 现在有2个消费者的时候，会各自消费一个分区，不会混乱
//        consumer2();

        // 现在有3个消费者的时候，会各自消费一个分区，但是有一个会空闲
//        consumer3();



    }



    /**
     *  主题为的分区为2个，而且我们的消费者都属于同一个组，现在有2个消费者的时候，会各自消费一个分区，不会混乱
     * result:
     * author: lwl
     * date: 2020/8/7 14:19
     */
    public static void consumer2(){
        /**
         *
         *  从这里可以看出 Thread-1 只消费了 partition = 0 分区的数据
         *              Thread-2 只消费了 partition = 1 分区的数据
         * thread = Thread-1  topic = multi.consumer.topic partition = 0 offset = 13, key = key-1, value = value-1
         *  thread = Thread-2  topic = multi.consumer.topic partition = 1 offset = 17, key = key-3, value = value-3
         *  thread = Thread-1  topic = multi.consumer.topic partition = 0 offset = 14, key = key-2, value = value-2
         *  thread = Thread-2  topic = multi.consumer.topic partition = 1 offset = 18, key = key-4, value = value-4
         *  thread = Thread-1  topic = multi.consumer.topic partition = 0 offset = 15, key = key-5, value = value-5
         *  thread = Thread-2  topic = multi.consumer.topic partition = 1 offset = 19, key = key-7, value = value-7
         *  thread = Thread-1  topic = multi.consumer.topic partition = 0 offset = 16, key = key-6, value = value-6
         *  thread = Thread-2  topic = multi.consumer.topic partition = 1 offset = 20, key = key-8, value = value-8
         *  thread = Thread-1  topic = multi.consumer.topic partition = 0 offset = 17, key = key-10, value = value-10
         *  thread = Thread-1  topic = multi.consumer.topic partition = 0 offset = 18, key = key-12, value = value-12
         *  thread = Thread-1  topic = multi.consumer.topic partition = 0 offset = 19, key = key-14, value = value-14
         *  thread = Thread-1  topic = multi.consumer.topic partition = 0 offset = 20, key = key-17, value = value-17
         *  thread = Thread-1  topic = multi.consumer.topic partition = 0 offset = 21, key = key-19, value = value-19
         *  thread = Thread-2  topic = multi.consumer.topic partition = 1 offset = 21, key = key-9, value = value-9
         *  thread = Thread-2  topic = multi.consumer.topic partition = 1 offset = 22, key = key-11, value = value-11
         *  thread = Thread-2  topic = multi.consumer.topic partition = 1 offset = 23, key = key-13, value = value-13
         *  thread = Thread-2  topic = multi.consumer.topic partition = 1 offset = 24, key = key-15, value = value-15
         *  thread = Thread-2  topic = multi.consumer.topic partition = 1 offset = 25, key = key-16, value = value-16
         *  thread = Thread-2  topic = multi.consumer.topic partition = 1 offset = 26, key = key-18, value = value-18
         */
//        // 创建2个kafka消费者对象
        KafkaConsumer c1 = getKafkaConsumer();
        KafkaConsumer c2 = getKafkaConsumer();

        // 创建2个线程
        KafkaConsumerRunner k1 = new KafkaConsumerRunner(c1);
        KafkaConsumerRunner k2 = new KafkaConsumerRunner(c2);

        new Thread(k1).start();
        new Thread(k2).start();


    }



    /**
     *  主题为的分区为2个，而且我们的消费者都属于同一个组，现在有2个消费者的时候，会各自消费一个分区，不会混乱
     * result:
     * author: lwl
     * date: 2020/8/7 14:19
     */
    public static void consumer3(){
        /**
         *
         *  可以看出，我们有3个消费者
         * 14:23:07.465 [Thread-1] DEBUG org.apache.kafka.clients.consumer.internals.AbstractCoordinator - [Consumer clientId=consumer-1, groupId=test] Sending JoinGroup ((type: JoinGroupRequest, groupId=test, sessionTimeout=10000, rebalanceTimeout=300000, memberId=, protocolType=consumer, groupProtocols=org.apache.kafka.common.requests.JoinGroupRequest$ProtocolMetadata@1154439)) to coordinator 192.168.31.28:9092 (id: 2147483647 rack: null)
         * 14:23:07.465 [Thread-2] DEBUG org.apache.kafka.clients.consumer.internals.AbstractCoordinator - [Consumer clientId=consumer-2, groupId=test] Sending JoinGroup ((type: JoinGroupRequest, groupId=test, sessionTimeout=10000, rebalanceTimeout=300000, memberId=, protocolType=consumer, groupProtocols=org.apache.kafka.common.requests.JoinGroupRequest$ProtocolMetadata@1d5f75d9)) to coordinator 192.168.31.28:9092 (id: 2147483647 rack: null)
         * 14:23:07.465 [Thread-3] DEBUG org.apache.kafka.clients.consumer.internals.AbstractCoordinator - [Consumer clientId=consumer-3, groupId=test] Sending JoinGroup ((type: JoinGroupRequest, groupId=test, sessionTimeout=10000, rebalanceTimeout=300000, memberId=, protocolType=consumer, groupProtocols=org.apache.kafka.common.requests.JoinGroupRequest$ProtocolMetadata@44b3f48d)) to coordinator 192.168.31.28:9092 (id: 2147483647 rack: null)
         * 1
         *
         *  从这里可以看出 Thread-1 只消费了 partition = 0 分区的数据
         *              Thread-2 只消费了 partition = 1 分区的数据
         *              Thread-3 什么也没有消费，属于空闲状态
         *  thread = Thread-2  topic = multi.consumer.topic partition = 1 offset = 27, key = key-3, value = value-3
         *  thread = Thread-1  topic = multi.consumer.topic partition = 0 offset = 22, key = key-1, value = value-1
         *  thread = Thread-2  topic = multi.consumer.topic partition = 1 offset = 28, key = key-4, value = value-4
         *  thread = Thread-1  topic = multi.consumer.topic partition = 0 offset = 23, key = key-2, value = value-2
         *  thread = Thread-2  topic = multi.consumer.topic partition = 1 offset = 29, key = key-7, value = value-7
         *  thread = Thread-1  topic = multi.consumer.topic partition = 0 offset = 24, key = key-5, value = value-5
         *  thread = Thread-2  topic = multi.consumer.topic partition = 1 offset = 30, key = key-8, value = value-8
         *  thread = Thread-1  topic = multi.consumer.topic partition = 0 offset = 25, key = key-6, value = value-6
         *  thread = Thread-2  topic = multi.consumer.topic partition = 1 offset = 31, key = key-9, value = value-9
         *  thread = Thread-1  topic = multi.consumer.topic partition = 0 offset = 26, key = key-10, value = value-10
         *  thread = Thread-2  topic = multi.consumer.topic partition = 1 offset = 32, key = key-11, value = value-11
         *  thread = Thread-1  topic = multi.consumer.topic partition = 0 offset = 27, key = key-1, value = value-1
         *  thread = Thread-2  topic = multi.consumer.topic partition = 1 offset = 33, key = key-3, value = value-3
         *  thread = Thread-1  topic = multi.consumer.topic partition = 0 offset = 28, key = key-2, value = value-2
         *  thread = Thread-2  topic = multi.consumer.topic partition = 1 offset = 34, key = key-4, value = value-4
         *  thread = Thread-1  topic = multi.consumer.topic partition = 0 offset = 29, key = key-5, value = value-5
         *  thread = Thread-2  topic = multi.consumer.topic partition = 1 offset = 35, key = key-7, value = value-7
         *  thread = Thread-1  topic = multi.consumer.topic partition = 0 offset = 30, key = key-6, value = value-6
         *  thread = Thread-2  topic = multi.consumer.topic partition = 1 offset = 36, key = key-8, value = value-8
         *  thread = Thread-1  topic = multi.consumer.topic partition = 0 offset = 31, key = key-10, value = value-10
         *  thread = Thread-2  topic = multi.consumer.topic partition = 1 offset = 37, key = key-9, value = value-9
         *  thread = Thread-2  topic = multi.consumer.topic partition = 1 offset = 38, key = key-11, value = value-11
         */
//        // 创建3个kafka消费者对象
        KafkaConsumer c1 = getKafkaConsumer();
        KafkaConsumer c2 = getKafkaConsumer();
        KafkaConsumer c3 = getKafkaConsumer();

        // 创建3个线程
        KafkaConsumerRunner k1 = new KafkaConsumerRunner(c1);
        KafkaConsumerRunner k2 = new KafkaConsumerRunner(c2);
        KafkaConsumerRunner k3 = new KafkaConsumerRunner(c3);

        new Thread(k1).start();
        new Thread(k2).start();
        new Thread(k3).start();


    }

    /**
     *  做些初始化的数据 创建主题
     * result:
     * author: lwl
     * date: 2020/8/7 14:18
     */
    public static void createTopic() {
        // 创建主题，有2个分区  每个分区有一个副本
//        AdminApi.topicAdd(TOPIC_NAME, 2, (short) 1);

        // 查看主题的描述
        /**
         *  key :multi.consumer.topic ,
         *  value :(
         *      name=multi.consumer.topic,
         *      internal=false,
         *      partitions=
         *          (partition=0, leader=192.168.31.28:9092 (id: 0 rack: null), replicas=192.168.31.28:9092 (id: 0 rack: null), isr=192.168.31.28:9092 (id: 0 rack: null)),
         *          (partition=1, leader=192.168.31.28:9092 (id: 0 rack: null), replicas=192.168.31.28:9092 (id: 0 rack: null), isr=192.168.31.28:9092 (id: 0 rack: null)))
         */
//        AdminApi.topicDesc(TOPIC_NAME);


    }



    public static KafkaConsumer getKafkaConsumer() {
        Properties props = ConsumerApi.getCommonPros();
        // 把自动提交设置为false 即可
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        // 消费者对象
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(props);
        return consumer;
    }


}

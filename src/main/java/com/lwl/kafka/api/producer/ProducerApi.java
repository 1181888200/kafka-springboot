package com.lwl.kafka.api.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.Future;

/**
 * @description： 生产者的api
 * @author     ：lwl
 * @date       ：2020/8/4 17:08
 * @version:     1.0.0
 */
public class ProducerApi {
    private static final String TOPIC_NAME = "admin.create.topic";


    public static void main(String[] args) throws Exception {

        // 异步发送消息
//        send();
        // 同步发送
//        sendSync();

        // 异步发送 带回调函数
        sendWithCallBack();


    }



    /**
     *  异步发送消息
     * result:
     * author: lwl
     * date: 2020/8/4 17:18
     */
    public static void send() throws Exception {
        Properties props = new Properties();
        //服务地址
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.31.28:9092");
        //失败重试次数
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        //批量发送数量
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, "4096");
        //延时时间，延时时间到达之后，批量发送数量没达到也会发送消息
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        //缓冲区的大小
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "409608");
        //序列化
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        // producer对象
        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        // 消息对象  recorder
        for (int i=1;i<10;i++){
            ProducerRecord<String,String> record =
                    new ProducerRecord<String, String>(TOPIC_NAME,"key-"+i,"value-"+i);
            // 发送消息
            producer.send(record);
        }
        // 关闭通道
        producer.close();

    }


    /**
     *  异步阻塞发送消息（同步发送）
     * result:
     * author: lwl
     * date: 2020/8/4 17:18
     */
    public static void sendSync() throws Exception {
        Properties props = new Properties();
        //服务地址
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.31.28:9092");
        //失败重试次数
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        //批量发送数量
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, "4096");
        //延时时间，延时时间到达之后，批量发送数量没达到也会发送消息
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        //缓冲区的大小
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "409608");
        //序列化
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        // producer对象
        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        // 消息对象  recorder
        for (int i=20;i<25;i++){
            ProducerRecord<String,String> record =
                    new ProducerRecord<String, String>(TOPIC_NAME,"key-"+i,"value-"+i);
            // 发送消息
            Future<RecordMetadata> send = producer.send(record);
            // get() 方法会阻塞该请求，直到有返回
            RecordMetadata recordMetadata = send.get();
            System.out.println("topic  : " + recordMetadata.topic()+ "  hasOffset  : " + recordMetadata.hasOffset()
                    + "  checksum  : " + recordMetadata.checksum()
                    + "  partition  : " + recordMetadata.partition()
                    + "  offset  : " + recordMetadata.offset());
        }
        // 关闭通道
        producer.close();

    }



    /**
     *  异步发送消息 带回调
     * result:
     * author: lwl
     * date: 2020/8/4 17:18
     */
    public static void sendWithCallBack() throws Exception {
        Properties props = new Properties();
        //服务地址
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.31.28:9092");
        //失败重试次数
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        //批量发送数量
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, "4096");
        //延时时间，延时时间到达之后，批量发送数量没达到也会发送消息
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        //缓冲区的大小
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "409608");
        //序列化
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        // producer对象
        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        // 消息对象  recorder
        for (int i=1;i<10;i++){
            ProducerRecord<String,String> record =
                    new ProducerRecord<String, String>(TOPIC_NAME,"key-"+i,"value-"+i);
            // 发送消息
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    System.out.println("topic  : " + recordMetadata.topic()+ "  hasOffset  : " + recordMetadata.hasOffset()
                            + "  checksum  : " + recordMetadata.checksum()
                            + "  partition  : " + recordMetadata.partition()
                            + "  offset  : " + recordMetadata.offset());
                }
            });
        }
        // 关闭通道
        producer.close();

    }



}

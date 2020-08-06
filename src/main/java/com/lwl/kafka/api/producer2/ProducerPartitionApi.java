package com.lwl.kafka.api.producer2;


import com.lwl.kafka.api.admin.AdminApi;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @description： 自定义分区器
 * @author     ：lwl
 * @date       ：2020/8/6 16:14
 * @version:     1.0.0
 */
public class ProducerPartitionApi {

    private static final String TOPIC_NAME = "define_partition";


    public static void main(String[] args) throws Exception {

        // 第一步：先创建主题
//        AdminApi.topicAdd(TOPIC_NAME, 2, (short) 1);

        // 第二步： 查看主题信息
        /**
         *  key :define_partition ,
         *  value :(
         *      name=define_partition,
         *      internal=false,
         *      partitions=(partition=0, leader=192.168.31.28:9092 (id: 0 rack: null), replicas=192.168.31.28:9092 (id: 0 rack: null), isr=192.168.31.28:9092 (id: 0 rack: null)),
         *                  (partition=1, leader=192.168.31.28:9092 (id: 0 rack: null), replicas=192.168.31.28:9092 (id: 0 rack: null), isr=192.168.31.28:9092 (id: 0 rack: null)))
         *
         */
//        AdminApi.topicDesc(TOPIC_NAME);



        // 第三步：发送消息
        /**
         * topic  : define_partition  hasOffset  : true  checksum  : 1666750207  partition  : 0  offset  : 0
         * topic  : define_partition  hasOffset  : true  checksum  : 2395557022  partition  : 0  offset  : 1
         * topic  : define_partition  hasOffset  : true  checksum  : 4057624595  partition  : 0  offset  : 2
         * topic  : define_partition  hasOffset  : true  checksum  : 474720882  partition  : 0  offset  : 3
         * topic  : define_partition  hasOffset  : true  checksum  : 680742911  partition  : 1  offset  : 0
         * topic  : define_partition  hasOffset  : true  checksum  : 2063047382  partition  : 1  offset  : 1
         * topic  : define_partition  hasOffset  : true  checksum  : 2395557022  partition  : 1  offset  : 2
         * topic  : define_partition  hasOffset  : true  checksum  : 98870875  partition  : 1  offset  : 3
         * topic  : define_partition  hasOffset  : true  checksum  : 474720882  partition  : 1  offset  : 4
         */
//        sendWithCallBack();
    }





    /**
     *  异步发送消息 带回调
     * result:
     * author: lwl
     * date: 2020/8/4 17:18
     */
    public static void sendWithCallBack() {
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

        // 添加自己的分区器
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, MyPartition.class);

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

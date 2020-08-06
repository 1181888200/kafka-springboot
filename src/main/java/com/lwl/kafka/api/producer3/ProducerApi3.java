package com.lwl.kafka.api.producer3;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @description： 消息的重要参数
 * @author     ：lwl
 * @date       ：2020/8/6 17:05
 * @version:     1.0.0
 */
public class ProducerApi3 {

    private static final String TOPIC_NAME = "producer_3_topic";



    public static void main(String[] args) {

        /**
         *
         * ProducerConfig.BATCH_SIZE_CONFIG,
         * batch.size
         *
            只要有多个记录被发送到同一个分区，生产者就会尝试将记录一起分成更少的请求。
            这有助于客户端和服务器的性能。该配置以字节为单位控制默认的批量大小。
            producer都是按照batch进行发送的，因此batch大小的选择对于producer性能至关重要。producer会把发往同一分区的多条消息封装进一个batch中，
                当batch满了后，producer才会把消息发送出去。但是也不一定等到满了，这和另外一个参数linger.ms有关。默认值为16K，合计为16384.
         */

        /**
         *
         * ProducerConfig.LINGER_MS_CONFIG,
         * linger.ms
         *
         *      生产者将在请求传输之间到达的任何记录归入单个批处理请求。通常情况下，这只会在记录到达速度快于发送时才发生。
         *      但是，在某些情况下，即使在中等负载下，客户端也可能希望减少请求的数量。此设置通过添加少量人工延迟来实现此目的
         *          - 即不是立即发送记录，而是生产者将等待达到给定延迟以允许发送其他记录，以便发送可以一起批量发送。
         *          这可以被认为与TCP中的Nagle算法类似。这个设置给出了批量延迟的上限：
         *          一旦我们得到batch.size值得记录的分区，它将被立即发送而不管这个设置如何，
         *          但是如果我们为这个分区累积的字节数少于这个数字，我们将在指定的时间内“等待”，等待更多的记录出现。该设置默认为0（即无延迟）。
         *          linger.ms=5例如，设置可以减少发送请求的数量，但会对在无效负载中发送的记录添加高达5毫秒的延迟。
         *          如果超过等待时间，分区积累的字节数任小于batch.size，消息也会被立马发送
         */

    }

    /**
     * acks参数：
     *     至多一次 ： acks=0如果设置为零，则生产者不会等待来自服务器的任何确认。该记录将被立即添加到套接字缓冲区并被视为已发送。
     *              在这种情况下，retries不能保证服务器已经收到记录，并且配置不会生效（因为客户端通常不会知道任何故障）。为每个记录返回的偏移量将始终设置为-1。
     *
     *     至少一次： acks=1这意味着领导者会将记录写入其本地日志中，但会在未等待所有追随者完全确认的情况下作出响应。
     *              在这种情况下，如果领导者在承认记录后但在追随者复制之前立即失败，那么记录将会丢失。
     *
     *     仅有一次： acks=all 等同于 -1 ，这意味着领导者将等待全套的同步副本确认记录。这保证只要至少有一个同步副本保持活动状态，记录就不会丢失。这是最强有力的保证。这相当于acks = -1设置。
     *     这个是最难做到的，会在发送消息的时候添加一个消息ID，然后取做去重的判断,需要消费者进行处理，由于ProducerRecord属性有限，可以把消息ID藏于value中，然后自定义序列化类，从中解析出来
     *
     *
     * result:
     * author: lwl
     * date: 2020/8/6 17:08
     */
    public static void send() throws Exception {
        Properties props = new Properties();
        //服务地址
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.31.28:9092");

        // 客户端ID
        // 发出请求时传递给服务器的id字符串。这样做的目的是通过允许将逻辑应用程序名称包含在服务器端请求日志中，从而能够跟踪ip / port之外的请求源，如果不手动指定，代码中会自动生成一个id。
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "client-id-1");

        // 消息确认模式
        props.put(ProducerConfig.ACKS_CONFIG, "all");

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

}

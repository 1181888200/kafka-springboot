package com.lwl.kafka.config;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafka
public class KafkaProducerConfig {
    @Value("${kafka.producer.servers}")
    private String servers;
    @Value("${kafka.producer.retries}")
    private int retries;
    @Value("${kafka.producer.batch.size}")
    private int batchSize;
    @Value("${kafka.producer.linger}")
    private int linger;
    @Value("${kafka.producer.buffer.memory}")
    private int bufferMemory;

    //参数设置
    public Map<String, Object> producerConfigs() {
        Map<String, Object> props = new HashMap<>();
        //服务地址
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        //失败重试次数
        props.put(ProducerConfig.RETRIES_CONFIG, retries);
        //批量发送数量
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, batchSize);
        //延时时间，延时时间到达之后，批量发送数量没达到也会发送消息
        props.put(ProducerConfig.LINGER_MS_CONFIG, linger);
        //缓冲区的大小
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, bufferMemory);
        //序列化
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return props;
    }

    //生产者工厂
    public ProducerFactory<String, String> producerFactory() {
        return new DefaultKafkaProducerFactory<>(producerConfigs());
    }

    //生产者template
    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        KafkaTemplate kafkaTemplate = new KafkaTemplate(producerFactory());
        return kafkaTemplate;
    }

}

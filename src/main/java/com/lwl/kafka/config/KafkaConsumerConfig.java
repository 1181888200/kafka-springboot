package com.lwl.kafka.config;

import com.lwl.kafka.service.MyListener;
import com.lwl.kafka.service.MyListenerAck;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafka
public class KafkaConsumerConfig {
    @Value("${kafka.consumer.servers}")
    private String servers;
    @Value("${kafka.consumer.enable.auto.commit}")
    private boolean enableAutoCommit;
    @Value("${kafka.consumer.session.timeout}")
    private String sessionTimeout;
    @Value("${kafka.consumer.auto.commit.interval}")
    private String autoCommitInterval;
    @Value("${kafka.consumer.group.id}")
    private String groupId;
    @Value("${kafka.consumer.auto.offset.reset}")
    private String autoOffsetReset;
    @Value("${kafka.consumer.concurrency}")
    private int concurrency;

    //自动确认参数设置
    public Map<String, Object> consumerConfigs() {
        Map<String, Object> propsMap = new HashMap<>();
        //服务地址
        propsMap.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        //自动提交
        propsMap.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, enableAutoCommit);
        //自动提交间隔
        propsMap.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, autoCommitInterval);
        //超时时间
        propsMap.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, sessionTimeout);
        //设置反序列化
        propsMap.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        propsMap.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        //群组id
        propsMap.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        //自动从最新的开始消费
        propsMap.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
        return propsMap;
    }

    //消费者工厂
    public ConsumerFactory<String, String> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(consumerConfigs());
    }

    //注入自动提交监听器
    @Bean("listener")
    public MyListener myListener() {
        return new MyListener();
    }

    //用于扩展监听器，使用时在@KafkaListener(containerFactory="bean的名字")即可
    @Bean("factory")
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, String>> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        //指定消费者
        factory.setConsumerFactory(consumerFactory());
        //设置并发量
        factory.setConcurrency(concurrency);
        //设置拉取等待时间
        factory.getContainerProperties().setPollTimeout(1500);
        return factory;
    }

    //手动确认参数设置
    public Map<String, Object> consumerConfigsAck() {
        Map<String, Object> propsMap = new HashMap<>();
        //服务地址
        propsMap.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        //关闭自动提交，设为手动提交
        propsMap.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        //反序列化
        propsMap.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        propsMap.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        //群组id
        propsMap.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        return propsMap;
    }

    public ConsumerFactory<String, String> consumerFactoryAck() {
        return new DefaultKafkaConsumerFactory<>(consumerConfigsAck());
    }

    //注入手动提交监听器
    @Bean("listenerAck")
    public MyListenerAck listenerAck() {
        return new MyListenerAck();
    }

    //用于扩展监听器，使用时在@KafkaListener(containerFactory="bean的名字")即可
    @Bean("factoryAck")
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, String>> kafkaListenerContainerFactoryAck() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        //指定消费者
        factory.setConsumerFactory(consumerFactoryAck());
        //设置并发量
        factory.setConcurrency(concurrency);
        //设置拉取等待时间
        factory.getContainerProperties().setPollTimeout(1500);
        //设置手动确认
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        return factory;
    }

}

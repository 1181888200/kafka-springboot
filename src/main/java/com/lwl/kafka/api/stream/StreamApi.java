package com.lwl.kafka.api.stream;


import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;

/**
 * @description： 流处理
 * @author     ：lwl
 * @date       ：2020/8/10 9:40
 * @version:     1.0.0
 */
public class StreamApi {


    private static final String INPUT_TOPIC = "input.topic";
    private static final String OUTPUT_TOPIC = "output.topic";


    public static void main(String[] args) throws Exception {


        // 创建拓扑
        StreamsBuilder builder = new StreamsBuilder();
        // 构建处理过程
        wordcountStream(builder);

        // 创建kafka流对象
        KafkaStreams streams = new KafkaStreams(builder.build(), getProperties());
        streams.start();

    }


    private static void wordcountStream(StreamsBuilder builder){
        // 从哪个主题开始读取数据
        KStream<String, String> source = builder.stream(INPUT_TOPIC);

        KTable<String, Long> count = source.flatMapValues(value -> Arrays.asList(value.toLowerCase(Locale.getDefault()).split(" ")))
                .groupBy((k, v) -> v)
                .count();


        count.toStream().to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));

    }



    private static Properties getProperties() {
        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.31.28:9092");
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG,"wordcount-app");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        return properties;
    }


}

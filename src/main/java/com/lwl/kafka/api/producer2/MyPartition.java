package com.lwl.kafka.api.producer2;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * @description： 实现自定义的分区器   == 负载均衡
 *              （数据推送到哪个主题的分区）
 * @author     ：lwl
 * @date       ：2020/8/6 16:05
 * @version:     1.0.0
 */
@Slf4j
public class MyPartition implements Partitioner {

    /**
     * key的前缀
     */
    private static final String KEY_PREFIX = "key-";

    /**
     * 假设分区有2个
     */
    private static final int PARTITION_COUNT = 2;

    @Override
    public int partition(String topic, Object key, byte[] bytes, Object o1, byte[] bytes1, Cluster cluster) {

        /**
         * 假设key的定义为  key-1, key-2, key-3
         *  然后我们的分区有2个，那么就可以做以下操作
         */
        // 获取原始的key
        String keyStr = key.toString();
        String keyInt = keyStr.replace(KEY_PREFIX, "");
        int value = Integer.valueOf(keyInt)%PARTITION_COUNT;
        log.info("");
        log.info("");
        log.info("原始的key: " + keyStr + " 对应的值： " + keyInt +" 路由到分区器："+ value);
        log.info("");
        log.info("");
        return value;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}

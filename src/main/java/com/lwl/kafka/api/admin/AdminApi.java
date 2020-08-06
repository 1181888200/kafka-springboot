package com.lwl.kafka.api.admin;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.config.ConfigResource;

import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * @description： admin的api
 * @author     ：lwl
 * @date       ：2020/8/4 17:08
 * @version:     1.0.0
 */
public class AdminApi {


    private static final String TOPIC_NAME = "admin.create.topic";


    public static void main(String[] args) throws ExecutionException, InterruptedException {
        // 创建adminApi对象
        AdminClient adminClient = adminClient();
        System.out.println("adminClient: " + adminClient);
        // 新增topic
//        topicAdd(adminClient);

        // 删除主题
//        delTopic(adminClient);

        // 获取所有的主题
//        topicList(adminClient);
//        topicListAll(adminClient);

        // 获取主题的描述
//        topicDesc(adminClient);

        // 获取主题的配置信息
//        describeConfig(adminClient);

        // 修改主题的配置信息
//        alterConfig(adminClient);

        // 添加分区
//        incrPartitions(adminClient, 2);
    }


    /**
     *  添加分区
     * @param adminClient
     * result:
     * author: lwl
     * date: 2020/8/4 17:00
     */
    public static void incrPartitions(AdminClient adminClient, int totalCount){
        Map<String, NewPartitions> map = new HashMap<>();
        NewPartitions newPartitions = NewPartitions.increaseTo(totalCount);
        map.put(TOPIC_NAME, newPartitions);
        CreatePartitionsResult partitions = adminClient.createPartitions(map);
    }


    /**
     *  修改config信息
     * @param adminClient
     * result:
     * author: lwl
     * date: 2020/8/4 16:54
     */
    public static void alterConfig(AdminClient adminClient)  {
        Map<ConfigResource, Config> map = new HashMap<>();
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, TOPIC_NAME);
        Config config = new Config(Arrays.asList(new ConfigEntry("preallocate","true")));
        map.put(resource,config);
        AlterConfigsResult alterConfigsResult = adminClient.alterConfigs(map);
    }

    /**
     *  获取主题的配置信息
     *       key :ConfigResource(type=TOPIC, name='admin.create.topic') ,
     *       value :Config(
     *          entries=[
     *                  ConfigEntry(name=compression.type, value=producer, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
     *                  ConfigEntry(name=leader.replication.throttled.replicas, value=, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
     *                  ConfigEntry(name=message.downconversion.enable, value=true, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
     *                  ConfigEntry(name=min.insync.replicas, value=1, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
     *                  ConfigEntry(name=segment.jitter.ms, value=0, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
     *                  ConfigEntry(name=cleanup.policy, value=delete, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
     *                  ConfigEntry(name=flush.ms, value=9223372036854775807, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
     *                  ConfigEntry(name=follower.replication.throttled.replicas, value=, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
     *                  ConfigEntry(name=segment.bytes, value=1073741824, source=STATIC_BROKER_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
     *                  ConfigEntry(name=retention.ms, value=604800000, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
     *                  ConfigEntry(name=flush.messages, value=9223372036854775807, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
     *                  ConfigEntry(name=message.format.version, value=2.5-IV0, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
     *                  ConfigEntry(name=file.delete.delay.ms, value=60000, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
     *                  ConfigEntry(name=max.compaction.lag.ms, value=9223372036854775807, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), 、
     *                  ConfigEntry(name=max.message.bytes, value=1048588, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
     *                  ConfigEntry(name=min.compaction.lag.ms, value=0, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
     *                  ConfigEntry(name=message.timestamp.type, value=CreateTime, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
     *                  ConfigEntry(name=preallocate, value=false, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
     *                  ConfigEntry(name=min.cleanable.dirty.ratio, value=0.5, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
     *                  ConfigEntry(name=index.interval.bytes, value=4096, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
     *                  ConfigEntry(name=unclean.leader.election.enable, value=false, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
     *                  ConfigEntry(name=retention.bytes, value=-1, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
     *                  ConfigEntry(name=delete.retention.ms, value=86400000, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
     *                  ConfigEntry(name=segment.ms, value=604800000, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
     *                  ConfigEntry(name=message.timestamp.difference.max.ms, value=9223372036854775807, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
     *                  ConfigEntry(name=segment.index.bytes, value=10485760, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[])
     *                  ])
     * @param adminClient
     * result:
     * author: lwl
     * date: 2020/8/4 16:47
     */
    public static void describeConfig(AdminClient adminClient) throws ExecutionException, InterruptedException {
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, TOPIC_NAME);
        DescribeConfigsResult describeConfigsResult = adminClient.describeConfigs(Arrays.asList(resource));
        System.out.println("输出所有的主题的配置信息----------------------------");
        Map<ConfigResource, Config> configResourceConfigMap = describeConfigsResult.all().get();
        configResourceConfigMap.entrySet().stream().forEach(r-> System.out.println(" key :" +r.getKey()+" , value :" + r.getValue()));
    }

    /**
     *  输出主题的描述
     *       key :admin.create.topic ,
     *       value :(
     *          name=admin.create.topic,
     *          internal=false,
     *          partitions=(
     *              partition=0,
     *              leader=192.168.31.28:9092
     *              (id: 0 rack: null),
     *              replicas=192.168.31.28:9092 (id: 0 rack: null),
     *              isr=192.168.31.28:9092 (id: 0 rack: null)))
     * @param adminClient
     * result:
     * author: lwl
     * date: 2020/8/4 16:42
     */
    public static void topicDesc(AdminClient adminClient) throws ExecutionException, InterruptedException {
        DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(Arrays.asList(TOPIC_NAME));
        System.out.println("输出所有的主题的描述----------------------------");
        Map<String, TopicDescription> stringTopicDescriptionMap = describeTopicsResult.all().get();
        stringTopicDescriptionMap.entrySet().stream().forEach(r-> System.out.println(" key :" +r.getKey()+" , value :" + r.getValue()));
    }




    /**
     *  删除topic
     * @param adminClient
     * result:
     * author: lwl
     * date: 2020/8/4 16:29
     */
    public static void delTopic(AdminClient adminClient){
        DeleteTopicsResult topics = adminClient.deleteTopics(Arrays.asList(TOPIC_NAME));
        System.out.println("DeleteTopicsResult : " + topics.toString());
    }

    /**
     *  打印出显示的主题
     * @param adminClient
     * result:
     * author: lwl
     * date: 2020/8/4 15:48
     */
    public static void topicList(AdminClient adminClient) throws ExecutionException, InterruptedException {
        ListTopicsResult listTopicsResult = adminClient.listTopics();
        System.out.println("输出所有的主题----------------------------");
        listTopicsResult.names().get().forEach(System.out::println);
    }

    /**
     *  打印出所有的主題，包括隱式的
     * @param adminClient
     * result:
     * author: lwl
     * date: 2020/8/4 15:49
     */
    public static void topicListAll(AdminClient adminClient) throws ExecutionException, InterruptedException {
        ListTopicsOptions options = new ListTopicsOptions();
        options.listInternal(true);
        ListTopicsResult listTopicsResult = adminClient.listTopics(options);
        System.out.println("输出所有的主题----------------------------");
        listTopicsResult.names().get().forEach(System.out::println);

        Collection<TopicListing> topicListings = listTopicsResult.listings().get();
        System.out.println("输出所有的主题对象----------------------------");
        topicListings.stream().forEach(System.out::println);

    }

    /**
     *  新增topic
     * @param adminClient
     * result:
     * author: lwl
     * date: 2020/8/4 16:32
     */
    public static void topicAdd(AdminClient adminClient) throws ExecutionException, InterruptedException {
        NewTopic newTopic = new NewTopic(TOPIC_NAME, 1, (short) 1);
        CreateTopicsResult topics = adminClient.createTopics(Arrays.asList(newTopic));
        System.out.println("CreateTopicsResult : " + topics.toString());
    }

    /**
     *  创建adminApi对象
     * result:
     * author: lwl
     * date: 2020/8/4 15:38
     */
    public static AdminClient adminClient() {
        Properties properties = new Properties();
        properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.31.28:9092");
        AdminClient admin =  AdminClient.create(properties);
        return admin;
    }

}

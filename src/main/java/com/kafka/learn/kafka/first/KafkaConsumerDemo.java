package com.kafka.learn.kafka.first;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * @功能职责:
 * @描述：Kafka消费者Demo
 * @作者: 郭辉
 * @创建时间: 2020-12-02 13:53
 * @copyright Copyright (c) 2020 中国软件与技术服务股份有限公司
 * @company 中国软件与技术服务股份有限公司
 */
public class KafkaConsumerDemo {

    private static final String brokerList = "localhost:9092";

    private static final String topic = "firstTopic";

    private static final String groupId = "group.demo";

    public static void main(String[] args) {
        Properties properties = new Properties();
        /**
         * 键反序列化
         * */
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        /**
         * 值得反序列化
         * */
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        /**
         *集群地址
         * */
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,brokerList);
        /**
         * 消费组
         * */
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        /**
         * 实例化消费者（用于消息的接受）
         * */
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);
        /**
         * 订阅topic
         * */
        consumer.subscribe(Collections.singleton(topic));

        while(true){
            /**
             * 一秒监听一次
             * poll（）方法是消息接受操作
             * ConsumerRecords封装了收到消息
             * */
            ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : records){
                System.out.println(record.value());
            }
        }

    }
}

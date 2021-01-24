package com.kafka.learn.kafka.first;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * @功能职责:
 * @描述：Kafka生产者Demo
 * @作者: 郭辉
 * @创建时间: 2020-12-02 13:53
 * @copyright Copyright (c) 2020 中国软件与技术服务股份有限公司
 * @company 中国软件与技术服务股份有限公司
 */
public class KafkaProducerDemo {

    private static final String brokerList = "localhost:9092";

    private static final String topic = "firstTopic";

    public static void main(String[] args) {
        Properties properties = new Properties();
        /**
         * 设置key序列化器
         * */
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        /**
         * 设置一个重试次数
         * */
        properties.put(ProducerConfig.RETRIES_CONFIG,10);
        /**
         * 设置值序列化器
         * */
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        /**
         * 设置集群地址
         * */
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,brokerList);

        /**
         * 消息发送者实例化
         * */
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);
        /**
         * 封装了发送的消息对象（包括消息的topic，消息内容等）
         * */
        ProducerRecord<String,String >record = new ProducerRecord<>(topic,"kafka-demo","hello1111 kafka");

        try {
            /**
             * 同步发送操作
             * */
            Future<RecordMetadata> send = producer.send(record);
            /**
             * RecordMetadata 封装了当前发送的消息的topic，分区，偏移量等
             * */
            RecordMetadata recordMetadata = send.get();
            //获取topic
            recordMetadata.topic();
            //分区
            recordMetadata.partition();
            //偏移量
            recordMetadata.offset();


            /**
             * 异步发送操作
             * */
            producer.send(record, new Callback() {
                /**
                 * 异步发送的毁掉接口
                 * */
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //获取topic
                    System.out.println(recordMetadata.topic());
                    //分区
                    System.out.println(recordMetadata.partition());
                    //偏移量
                    System.out.println(recordMetadata.offset());
                }
            });
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        producer.close();
    }
}

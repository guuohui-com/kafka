package com.kafka.learn.kafka.first;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @功能职责:
 * @描述：
 * @作者: 郭辉
 * @创建时间: 2020-12-02 13:53
 * @copyright Copyright (c) 2020 中国软件与技术服务股份有限公司
 * @company 中国软件与技术服务股份有限公司
 */
public class ProduceInceptor implements ProducerInterceptor<String,String> {

    private volatile long sendSuccess = 0;
    private volatile long sendFailure = 0;

    /**
     * 发送前的逻辑
     * */
    @Override
    public ProducerRecord onSend(ProducerRecord<String,String> producerRecord) {
        String value = "prefix-"+ producerRecord.value();
        return new ProducerRecord<>(producerRecord.topic(),producerRecord.partition(),producerRecord.timestamp(),producerRecord.key(),value);
    }

    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
        if (e == null){
            this.sendSuccess++;
        }else {
            this.sendFailure++;
        }
    }

    @Override
    public void close() {
        double rate = (double) sendSuccess/(sendFailure+sendSuccess);
        System.out.println("成功率为 %"+(rate*100));
    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}

package com.hainiuxy.kafka.jk;

import com.hainiuxy.kafka.KafkaData;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

public class MyPartitioner implements Partitioner {
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        KafkaData data = (KafkaData) value;
        String msg = data.msg();
        int num = Integer.parseInt(msg.split("_")[1]);
        return num % 2;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}

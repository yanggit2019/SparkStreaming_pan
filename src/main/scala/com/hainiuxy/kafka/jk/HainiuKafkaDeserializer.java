package com.hainiuxy.kafka.jk;

import com.hainiuxy.kafka.KafkaData;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.util.Map;

public class HainiuKafkaDeserializer implements Deserializer<KafkaData> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public KafkaData deserialize(String topic, byte[] data) {
        if(data == null) {
            return null;
        }
        // 把接收的消息反序列化
        ByteArrayInputStream bis = null;
        ObjectInputStream ois = null;
        Object obj = null;
        try{
            bis = new ByteArrayInputStream(data);
            ois = new ObjectInputStream(bis);
             obj = ois.readObject();
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            try{
                ois.close();
                bis.close();
            }catch (Exception e){
                e.printStackTrace();
            }
        }

        return (KafkaData)obj;
    }

    @Override
    public void close() {

    }
}

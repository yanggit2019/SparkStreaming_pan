package com.hainiuxy.kafka.jk;

import com.hainiuxy.kafka.KafkaData;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.Map;

/**
 *  自定义kafka消息 KafkaData 的序列化类
 */
public class HainiuKafkaSerializer implements Serializer<KafkaData> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }


    @Override
    public byte[] serialize(String topic, KafkaData data) {
        if(data == null){
            return null;
        }
        ByteArrayOutputStream bos = null;
        ObjectOutputStream oos = null;
        try{
            bos = new ByteArrayOutputStream();
            // 通过对象流把KafkaData 转成 Byte[]
            oos = new ObjectOutputStream(bos);
            oos.writeObject(data);
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            try{
                oos.close();
                bos.close();
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        return bos.toByteArray();
    }

    @Override
    public void close() {

    }
}

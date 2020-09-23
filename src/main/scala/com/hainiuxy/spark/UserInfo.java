package com.hainiuxy.spark;

import org.apache.hadoop.io.Text;



public class UserInfo{
    private String name = "hainiu"; // java实现了序列化

    private int age = 10;  // java实现了序列化

    transient private Text addr = new Text("beijing");  // 没有实现java的 Serializable接口

    public UserInfo() {
    }

    @Override
    public String toString() {
        return "UserInfo{" +
                "name='" + name + '\'' +
                ", age=" + age +
                ", addr=" + addr +
                '}';
    }
}

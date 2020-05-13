package com.xmg.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;

public class TestHdfsClient {
    public static void main(String[] args) throws Exception{
        Configuration configuration = new Configuration();
        //这里指定使用的是 hdfs 文件系统
        configuration.set("fs.defaultFS","hdfs://10.90.1.176:8020");
        //configuration.set("fs.defaultFS","hdfs://node-1:9000");
        System.setProperty("HADOOP_USER_NAME","root");
        FileSystem fileSystem = FileSystem.get(configuration);
        //fileSystem.create(new Path("helloByJava"));
        System.out.println(">>>>>>>>");
        fileSystem.close();

        Configuration conf = new Configuration();
        //conf.set( "fs.defaultFS" , "hdfs://10.90.1.176:50070" );
        FileSystem fs = FileSystem.get(new URI("hdfs://10.90.1.176:8020"), conf, "hdfs");
        System.out.println(">>>>>>>>");
        fs.close();
    }
}

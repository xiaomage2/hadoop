package com.xmg.hdfs;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.FileInputStream;
import java.net.URI;

public class TestHdfsClient {
    public static void main(String[] args) throws Exception{
        //8020  和 9000  FileSystem 端口，50070 是web端口
        Configuration configuration = new Configuration();
        //这里指定使用的是 hdfs 文件系统
        configuration.set("fs.defaultFS","hdfs://10.90.1.176:8020");
        //configuration.set("fs.defaultFS","hdfs://node-1:9000");
        //通过如下方式进行客户端身份的设置
        System.setProperty("HADOOP_USER_NAME","hdfs");

        //通过FileSystem 的静态方法获取文件系统的客户端对象
        FileSystem fs = FileSystem.get(configuration);
        //也可以通过如下的方式去指定文件系统的类型，兵器而同时设置用户的身份
        // FileSystem fs = FileSystem.get(new URI("hdfs://10.90.1.176:8020"), conf, "hdfs");
        //fileSystem.create(new Path("helloByJava"));

        //创建一个目录
        //fs.create(new Path("/hdfsByJava"));

        //上传一个文件
       // fs.copyFromLocalFile(new Path("h:/sparkTest/wordCount.txt"),new Path("/hdfsByJava/wordCount.txt"));

        //删除文件
        //fs.delete(new Path("/hdfsByJava/wordCount.txt"));

        //下载文件 文件目录会生成一个隐藏的 .crc 的校验文件 ，注意：windows 平台必须配置 hadoop 环境变量，其中hadoop 必须是在 winsows 环境下编译的版本，里面有个winutils.exe 文件
        //否则会报错
        //fs.copyToLocalFile(new Path("/hdfsByJava/wordCount.txt"),new Path("C:\\Users\\Administrator\\Desktop"));

        //使用Stream的形式，操作hdfs,更底层的方式
        FSDataOutputStream fsDataOutputStream = fs.create(new Path("/hdfsByJava/1.txt"), true);
        FileInputStream fileInputStream = new FileInputStream("h:/sparkTest/1.txt");
        IOUtils.copy(fileInputStream,fsDataOutputStream);

        System.out.println(">>>>>>>>");
        //关闭文件系统
        fs.close();

      /*  Configuration conf = new Configuration();
        //conf.set( "fs.defaultFS" , "hdfs://10.90.1.176:50070" );

        FileSystem fs = FileSystem.get(new URI("hdfs://10.90.1.176:8020"), conf, "hdfs");
        System.out.println(">>>>>>>>");
        fs.close();*/
    }


}

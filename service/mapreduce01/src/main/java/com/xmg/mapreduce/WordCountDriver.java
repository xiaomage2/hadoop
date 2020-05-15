package com.xmg.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 这个类就是 MR 程序运行时候的主类，本来中组装了一些程序运行时候所需要的信息
 * 比如：使用的是哪个 mapper 类，哪个 reduce 类 输入数据在哪里，输出数据在哪里
 */
public class WordCountDriver {

    public static void main(String[] args) throws Exception{
        //通过 Job 类来封装本次的 mr 的相关程序
        Configuration conf = new Configuration();

        //本地模式跑任务  默认是 local  模式
        //conf.set("mapreduce.framework.name","local");

        Job job = Job.getInstance(conf);

        //指定本次 mrjob  jar 包的运行主类
        job.setJarByClass(WordCountDriver.class);

        //指定本次 mr 所用的 mapper reducer 类分别是什么
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        //指定本次 mr  mapper 阶段的输出 k v 类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //指定本次 mr  reducer 阶段的最终输出 k v 类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //指定本次 mr 输入的数据的 路径 和 输出结果 存在什么位置
        FileInputFormat.setInputPaths(job,"/hdfsByJava/wordCount/input");
        FileOutputFormat.setOutputPath(job,new Path("/hdfsByJava/wordCount/output"));

        //job.submit();
        // 提交程序 并且监控 打印程序执行情况
        boolean b = job.waitForCompletion(true);
        System.exit(b?0:1);
    }
}

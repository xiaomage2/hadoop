package com.xmg.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT>
 * 这里是mr程序 reducer阶段处理的类
 * KEYIN：就是reducer阶段输入的数据 key 类型，对应的mapper的输出key类型，在本案例中就是单词 Text
 * VALUEIN：就是reducer阶段输入的数据 value 类型，对应的mapper的输出 value 类型，在本案例中就是单词次次数 intWritable
 * KEYOUT：就是reducer阶段输入的数据 key 类型，在本案例中就是单词 Text
 * VALUEOUT：就是reducer阶段输入的数据 value 类型，在本案例中就是单词的总次数 intWritable
 */
public class WordCountReducer extends Reducer<Text, IntWritable,Text,IntWritable> {

    /**
     * 这里是 reduce 阶段具体业务类的实现方法
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     *
     * reduce 接收所有来自 map 阶段处理的数据之后，按照 key 的字典序进行排序
     * <hello,1><hadoop,1><spark,1><hello,1><hadoop,1>
     *     排序后：<hadoop,1> <hadoop,1> <hello,1> <hello,1> <spark,1>
     * 按照 key 是否相同作为一组去调用 reduce 方法
     * 本方法的 key 就是这一组相同kv对的共同key
     * 把这一组所有的 v 作为一个迭代器传入我们的 reduce  方法
     *      <hadoop,[1,1]><hello,[1,1]><spark,1>
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //定义一个计数器
        int count = 0 ;
        //遍历一组迭代器，把每一个数量1 累加起来，就构成了单词的总次数
        for (IntWritable value:values) {
            count+=value.get();
        }
        //把最终的结果输出
        context.write(key,new IntWritable(count));
    }
}

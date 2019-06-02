package com.xuxin.bigdata.hadoop.mr.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


/**
 * KEYIN：Map任务读数据的key类型，offset，是每行数据起始位置的偏移量，Long
 * VALUEIN：Map任务读数据的value类型，其实就是一行行的字符串，String
 *
 * KEYOUT：Map方法自定义实现输出的key的类型，String
 * VALUEOUT：Map方法自定义实现输出的value的类型，Int
 *
 * 使用Hadoop自定义类型，较好地支持序列化和反序列化
 * LongWritable，Text，IntWritable
 *
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // 把value对应的行数据按照指定的分隔符拆开
        String[] words = value.toString().split("\t");

        for (String word : words) {
            // 变成 (hello,1) (word,1) 这种形式
            context.write(new Text(word.toLowerCase()), new IntWritable(1));
        }
    }
}

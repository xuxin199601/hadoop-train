package com.xuxin.bigdata.hadoop.mr.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * Combiner操作
 */
public class WordCountCombinerLocalApp {


    public static void main(String[] args) throws Exception{
        Configuration configuration = new Configuration();

        // 创建一个Job
        Job job = Job.getInstance(configuration);

        // 设置Job对应的参数: 主类
        job.setJarByClass(WordCountCombinerLocalApp.class);

        // 设置Job对应的参数: 设置自定义的Mapper和Reducer处理类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // 添加Combiner的设置
        job.setCombinerClass(WordCountReducer.class);

        // 设置Job对应的参数: Mapper输出key和value的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 设置Job对应的参数: Reduce输出key和value的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 如果输出目录已存在，则先删除
        FileSystem fileSystem =  FileSystem.get(configuration);
        Path outputPath = new Path("output");
        if (fileSystem.exists(outputPath)) {
            fileSystem.delete(outputPath, true);
        }


        // 设置Job对应的参数: Mapper输出key和value的类型：作业输入和输出的路径
        FileInputFormat.setInputPaths(job, new Path("input/wc.input"));
        FileOutputFormat.setOutputPath(job, new Path("output"));

        // 提交job
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : -1);

    }
}


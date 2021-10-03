package com.zhongbin.mapreduce.outputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class LogDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        //1. 获取job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //2. 设置jar路径
        job.setJarByClass(LogDriver.class);

        //3.关闭map和reduce
        job.setMapperClass(LogMapper.class);
        job.setReducerClass(LogReducer.class);

        //4.设置map输出的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        //5.设置最终输出的KV类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setOutputFormatClass(LogOutputFormat.class);

        //6.设置输入路径和输出路径
        FileInputFormat.setInputPaths(job, new Path("C:\\Users\\HP\\Desktop\\Hadoop\\资料\\资料\\11_input\\inputoutputformat"));
        FileOutputFormat.setOutputPath(job, new Path("C:\\Users\\HP\\Desktop\\Hadoop\\资料\\资料\\_output\\inputoutputformat_S"));

        //7.提交job
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);

    }
}

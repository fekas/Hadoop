package com.zhongbin.mapreduce.etl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class WebLogDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {

        args = new String[]{"C:\\Users\\HP\\Desktop\\Hadoop\\资料\\资料\\11_input\\inputlog", "C:\\Users\\HP\\Desktop\\Hadoop\\资料\\资料\\_output\\WebLogETL"};

        //1. 获取job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //2. 设置jar路径
        job.setJarByClass(WebLogDriver.class);

        //3.关闭map和reduce
        job.setMapperClass(WebLogMapper.class);

        //4.设置map输出的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        //5.设置最终输出的KV类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //map端join不需要reduce阶段，设置reduceTask为0
        job.setNumReduceTasks(0);

        //6.设置输入路径和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //7.提交job
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }
}

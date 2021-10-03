package com.zhongbin.mapreduce.compress;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * KEYIN    mape阶段输入的key的类型：Long类型（偏移量）
 * VALUEIN  map阶段输入value类型：Text
 * KEYOUT   map阶段输出的key类型：Text
 * VALUEOUT map阶段输出的value类型：IntWritable
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Text outKey = new Text();
    private IntWritable outValue = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取一行
        String line = value.toString();

        //分割
        String[] words = line.split(" ");

        //循环写出
        for (String word : words) {

            outKey.set(word);

            context.write(outKey, outValue);
        }
    }
}

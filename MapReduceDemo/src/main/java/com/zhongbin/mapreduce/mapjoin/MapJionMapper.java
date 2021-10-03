package com.zhongbin.mapreduce.mapjoin;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

public class MapJionMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    private HashMap<String, String> pdMap = new HashMap<>();
    private Text outK = new Text();

    //获取缓存文件并把文件内容封装到集合
    protected void setup(Context context) throws IOException, InterruptedException {

        URI[] cacheFiles = context.getCacheFiles();

        FileSystem fs = FileSystem.get(context.getConfiguration());

        FSDataInputStream fis = fs.open(new Path(cacheFiles[0]));

        BufferedReader reader = new BufferedReader(new InputStreamReader(fis, "UTF-8"));

        String line;
        while(StringUtils.isNotEmpty(line = reader.readLine())){
            String[] split = line.split("\t");

            pdMap.put(split[0], split[1]);
        }

        IOUtils.closeStream(reader);
    }

    //处理order.txt
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();

        String[] split = line.split("\t");

        String pname = pdMap.get(split[1]);

        outK.set(split[0] + "\t" + pname + "\t" + split[2]);

        context.write(outK, NullWritable.get());
    }
}

package com.zhongbin.mapreduce.outputformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class LogRecordWriter extends RecordWriter<Text, NullWritable> {

    private FSDataOutputStream atguiguOut;
    private FSDataOutputStream otherOut;

    public LogRecordWriter(TaskAttemptContext job) {
        //创建两条流

        try {
            FileSystem fs = FileSystem.get(job.getConfiguration());

            atguiguOut = fs.create(new Path("C:\\Users\\HP\\Desktop\\Hadoop\\资料\\资料\\_output\\atguigu.log"));

            otherOut = fs.create(new Path("C:\\Users\\HP\\Desktop\\Hadoop\\资料\\资料\\_output\\other.log"));

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void write(Text text, NullWritable nullWritable) throws IOException, InterruptedException {

        String log = text.toString();

        //写出
        if(log.contains("atguigu")){
            atguiguOut.writeBytes(log + "\n");
        }else {
            otherOut.writeBytes(log + "\n");
        }
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

        IOUtils.closeStream(atguiguOut);
        IOUtils.closeStream(otherOut);
    }
}

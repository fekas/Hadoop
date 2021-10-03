package com.zhongbin.mapreduce.partitioner2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

    private FlowBean outV = new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {

        //遍历集合累加值
        long totalUp = 0;
        long totaldown = 0;

        for (FlowBean value : values) {
            totalUp += value.getUpflow();
            totaldown += value.getDownflow();
        }

        //封装outK,outV
        outV.setUpflow(totalUp);
        outV.setDownflow(totaldown);
        outV.setSumflow();

        //写出
        context.write(key, outV);
    }
}

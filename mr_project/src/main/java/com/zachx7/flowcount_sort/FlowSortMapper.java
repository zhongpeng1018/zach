package com.zachx7.flowcount_sort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author zach - 吸柒
 */
public class FlowSortMapper extends Mapper<LongWritable,Text,FlowBean,Text> {

    private FlowBean fb =new FlowBean();
    private Text k = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        String[] fields = line.split("\t");

        k.set(fields[0]); //手机号码

        fb.setUpFlow(Integer.parseInt(fields[1]));
        fb.setDownFlow(Integer.parseInt(fields[2]));
        fb.setUpCountFlow(Integer.parseInt(fields[3]));
        fb.setDownCountFlow(Integer.parseInt(fields[4]));

        context.write(fb,k);

    }
}

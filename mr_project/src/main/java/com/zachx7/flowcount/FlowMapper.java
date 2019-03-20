package com.zachx7.flowcount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author zach - 吸柒
 */
public class FlowMapper extends Mapper<LongWritable,Text,Text,FlowBean> {

    private FlowBean fb = new FlowBean();
    private Text k = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1363157985066 	13726230503	00-FD-07-A4-72-B8:CMCC	120.196.100.82	i02.c.aliimg.com		24	27	2481	24681	200
        String line = value.toString();

        String[] fields = line.split("\t");

        k.set(fields[1]); //手机号码

        fb.setUpFlow(Integer.parseInt(fields[6]));
        fb.setDownFlow(Integer.parseInt(fields[7]));
        fb.setUpCountFlow(Integer.parseInt(fields[8]));
        fb.setDownCountFlow(Integer.parseInt(fields[9]));

        context.write(k,fb);

    }
}

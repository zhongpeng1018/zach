package com.zachx7.partition;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author zach - 吸柒
 */
public class DataMapper extends Mapper<LongWritable, Text, IntWritable, Text>{

    private IntWritable k = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1	0	1	2017-07-31 23:10:12	837255	6	4+1+1=6	小,双	0	0.00	0.00	1	0.00	1	1

        String line = value.toString();
        String[] fields = line.split("\t");
        k.set(Integer.parseInt(fields[5]));
        context.write(k,value);
    }
}

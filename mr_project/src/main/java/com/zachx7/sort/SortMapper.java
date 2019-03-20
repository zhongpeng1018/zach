package com.zachx7.sort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author zach - 吸柒
 */
public class SortMapper extends Mapper<LongWritable,Text,JavaBean,NullWritable> {

    private JavaBean jb = new JavaBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split("\t");
        jb.setFirst(fields[0]);
        jb.setSecond(Integer.parseInt(fields[1]));
        context.write(jb,NullWritable.get());
    }
}

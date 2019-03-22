package com.zachx7.index;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @author zach - 吸柒
 */
public class IndexMapper extends Mapper<LongWritable,Text,Text,IntWritable> {

    private Text k = new Text();
    private IntWritable v = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        FileSplit split = (FileSplit) context.getInputSplit();

        String filename = split.getPath().getName();

        String line = value.toString();

        String[] fields = line.split(" ");

        for (String field : fields) {
            k.set(field + "-" + filename);
            v.set(1);
            context.write(k,v);
        }

    }
}

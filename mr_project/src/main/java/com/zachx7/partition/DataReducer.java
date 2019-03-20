package com.zachx7.partition;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


/**
 * @author zach - 吸柒
 */
public class DataReducer extends Reducer<IntWritable,Text, Text, NullWritable> {
    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        for (Text k : values) {
            context.write(k, NullWritable.get());
        }

    }
}

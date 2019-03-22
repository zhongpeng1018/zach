package com.zachx7.index;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author zach - 吸柒
 */
public class IndexReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

    private IntWritable v = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int count = 0;

        for (IntWritable value : values) {
            count += value.get();
        }

        v.set(count);

        context.write(key,v);
    }
}

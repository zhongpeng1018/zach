package com.zachx7.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author zach - 吸柒
 */
public class WordReducer extends Reducer<Text, IntWritable,Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int count = 0;

        for (IntWritable value : values) {
            count+=value.get();
        }

        System.out.println(key.toString()+"--"+count);

        context.write(key,new IntWritable(count));

    }
}

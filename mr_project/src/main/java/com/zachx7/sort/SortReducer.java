package com.zachx7.sort;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author zach - 吸柒
 */
public class SortReducer extends Reducer<JavaBean,NullWritable,JavaBean,NullWritable> {

    @Override
    protected void reduce(JavaBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        for (NullWritable value : values) {
            context.write(key,value);
        }
    }
}

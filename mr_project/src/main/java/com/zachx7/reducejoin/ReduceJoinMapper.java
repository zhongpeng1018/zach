package com.zachx7.reducejoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @author zach - 吸柒
 *
 *  1001,20150710,p0001,2
    1004,20150710,p0001,2
    1002,20150710,p0002,3
    1002,20150710,p0003,3

    p0001,小米5,1000,2000  1001,20150710,p0001,2 1004,20150710,p0001,2
    p0002,锤子T1,1000,3000
 *
 *
 */
public class ReduceJoinMapper extends Mapper<LongWritable,Text,Text,Text> {

    Text k = new Text();
    Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        FileSplit split = (FileSplit) context.getInputSplit();
        String filename = split.getPath().getName();

        String line = value.toString();

        String[] fields = line.split(",");
        if(fields != null && fields.length >0){
            if("product.txt".equals(filename)){
                k.set(fields[0]);
            }else{
                k.set(fields[2]);
            }

            v.set(line);

            context.write(k,v);

        }
    }
}

package com.zachx7.partition;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author zach - 吸柒
 */
public class MyPartitioner extends Partitioner<IntWritable,Text>{
    @Override
    public int getPartition(IntWritable key, Text value, int numReduceTask) {

        try {
            if(key.get()>15){
                return 0;
            }else if (key.get()<15){
                return 1;
            }else {
                return 2;
            }
        } catch (NumberFormatException e) {
          return 2;
        }

    }
}

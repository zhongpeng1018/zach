package com.zachx7.commonfriend;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author zach - 吸柒
 */
public class FriendOneMapper extends Mapper<LongWritable,Text,Text,Text> {

    Text k = new Text();
    Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //A:B,C,D,F,E,O  预计输出格式 B-A C-A D-A F-A E-A O-A

        String line = value.toString();

        String[] split = line.split(":");

        v.set(split[0]); //保存该用户

        String[] friends = split[1].split(",");

        for (String friend : friends) {
            k.set(friend);
            context.write(k,v);
        }

    }
}

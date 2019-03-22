package com.zachx7.commonfriend;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;


/**
 * @author zach - 吸柒
 */
public class FriendTwoMapper extends Mapper<LongWritable,Text,Text,Text> {

    Text k = new Text();
    Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //I-K-C-B-G-F-H-O-D-	A

        String line = value.toString();
        String[] fields= line.split("\t");
        String[] users = fields[0].split("-");
        Arrays.sort(users); //排序，防止出现顺序不一致
        v.set(fields[1]);
        for (int i = 0 ; i < users.length;i++){ //A B C
            for(int j = i+1 ; j< users.length ; j++){
                k.set(users[i]+"-"+users[j]);
                context.write(k,v);
            }
        }


    }
}

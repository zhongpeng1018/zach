package com.zachx7.commonfriend;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author zach - 吸柒
 */
public class FriendTwoReducer extends Reducer<Text,Text,Text,Text>{

    Text v = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        StringBuffer sb = new StringBuffer();

        for (Text value : values) {
            sb.append(value.toString()).append("-");
        }

        String str = sb.toString();
        v.set(str.substring(0, str.lastIndexOf("-")));

        context.write(key,v);

    }
}

package com.zachx7.commonfriend;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author zach - 吸柒
 *  预计输出格式：B
 */
public class FriendOneReducer extends Reducer<Text,Text,Text,Text> {

    Text k = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        StringBuffer sb = new StringBuffer();

        for (Text value : values) {
            sb.append(value.toString()).append("-");
        }

        k.set(sb.toString());

        context.write(k,key);

    }
}

package com.zachx7.reducejoin;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author zach - 吸柒
 */
public class ReduceJoinReducer extends Reducer<Text,Text,Text,Text> {


    Text v = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        String first = "";
        List<String> list = new ArrayList<>();

        for (Text value : values) {
            if(value.toString().startsWith("p")){
                first = value.toString();
            }else{
                list.add(value.toString());
            }
        }

        for (String s : list) {
            v.set(first + "\t"+s);
            context.write(key,v);
        }

    }
}

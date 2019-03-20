package com.zachx7.sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author zach - 吸柒
 */
public class SortDriver extends Configured implements Tool {

    private static Configuration conf;

    static {
        conf = new Configuration();
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(conf);

        job.setJarByClass(SortDriver.class);

        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortReducer.class);

        job.setMapOutputKeyClass(JavaBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(JavaBean.class);
        job.setOutputValueClass(NullWritable.class);

        job.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        boolean result = job.waitForCompletion(true);

        return result?0:1;
    }

    public static void main(String[] args) {

        try {
            int status = ToolRunner.run(conf, new SortDriver(), args);
            System.exit(status);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}

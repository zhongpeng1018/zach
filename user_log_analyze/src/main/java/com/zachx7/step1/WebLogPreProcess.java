package com.zachx7.step1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import javax.xml.soap.Text;

/**
 * @author zach - 吸柒
 *  用户行为分析第一步：预处理数据
 */
public class WebLogPreProcess extends Configured implements Tool{

    private static Configuration conf = new Configuration();

    @Override
    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance(conf);

        job.setJarByClass(WebLogPreProcess.class);
        job.setMapperClass(WeblogPreMapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setNumReduceTasks(0);

        FileInputFormat.setInputPaths(job,new Path("D:\\用户行为分析\\input"));
        FileOutputFormat.setOutputPath(job,new Path("D:\\用户行为分析\\step1-clean"));

        boolean b = job.waitForCompletion(true);
        return b?0:1;
    }

    public static void main(String[] args) {
        try {
            int run = ToolRunner.run(conf, new WebLogPreProcess(), args);
            System.exit(run);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

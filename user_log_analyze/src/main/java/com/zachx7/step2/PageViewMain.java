package com.zachx7.step2;

import com.zachx7.bean.PageViewsBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author zach - 吸柒
 */
public class PageViewMain extends Configured implements Tool {
    private static Configuration conf = new Configuration();

    @Override
    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance(conf);

        job.setJarByClass(PageViewMain.class);
        job.setMapperClass(PageViewMapper.class);
        job.setReducerClass(PageViewReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PageViewsBean.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job,new Path("D:\\用户行为分析\\step1-clean"));
        FileOutputFormat.setOutputPath(job,new Path("D:\\用户行为分析\\step2-clean"));

        boolean b = job.waitForCompletion(true);
        return b?0:1;
    }

    public static void main(String[] args) {
        try {
            int run = ToolRunner.run(conf, new PageViewMain(), args);
            System.exit(run);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

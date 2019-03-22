package com.zachx7.mapjoin_hdfs;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * @author zach - 吸柒
 */
public class MapJoinMapper extends Mapper<LongWritable, Text, Text, Text> {

    private BufferedReader bis;

    private Map<String, String> map = new HashMap<>();

    private Text k = new Text();

    private Text v = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        URI[] files = context.getCacheFiles();

        FileSystem fileSystem = FileSystem.get(files[0],context.getConfiguration());

        FSDataInputStream fis = fileSystem.open(new Path(files[0]));

        bis = new BufferedReader(new InputStreamReader(fis));

        String line;

        while ((line = bis.readLine()) != null) {
            String[] fields = line.split(",");
            map.put(fields[0], line);
        }
        bis.close();
        fis.close();
        fileSystem.close();

    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();

        String[] split = line.split(",");

        k.set(split[2]);

        v.set(map.get(split[2]) + "\t" + line);

        context.write(k, v);

    }
}

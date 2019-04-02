package com.zachx7.step3;

import com.zachx7.bean.PageViewsBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author zach - 吸柒
 */
public class VisitMapper extends Mapper<LongWritable, Text, Text, PageViewsBean> {
    /**
     * private String session;
     * private String remote_addr;
     * private String inTime;
     * private String outTime;
     * private String inPage;
     * private String outPage;
     * private String referal;
     * <p>
     * df77fb70-8923-4f4c-954a-fd5086bcb088112.65.193.162013-09-18 08:48:31/hadoop-mahout-roadmap/160"-""Mozilla/4.0"38590200
     */

    Text k = new Text();
    PageViewsBean v = new PageViewsBean();

//    //777c4dd3-4ed0-4a9f-a622-b3960843a449  101.226.167.201  2013-09-18 09:30:36  /hadoop-mahout-roadmap/  1  60 "http://blog.fens.me/hadoop-mahout-roadmap/""Mozilla/4.0(compatible;MSIE8.0;WindowsNT6.1;Trident/4.0;SLCC2;.NETCLR2.0.50727;.NETCLR3.5.30729;.NETCLR3.0.30729;MediaCenterPC6.0;MDDR;.NET4.0C;.NET4.0E;.NETCLR1.1.4322;TabletPC2.0);360Spider"10335200
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\001");
        if (fields.length > 0) {
            k.set(fields[0]);
            v.setSession(fields[0]);
            v.setRemote_addr(fields[1]);
            v.setTimestr(fields[2]);
            v.setRequest(fields[3]);
            v.setStep(Integer.parseInt(fields[5]));
            v.setStaylong(fields[6]);
            v.setReferal(fields[7]);
            v.setUseragent(fields[8]);
            v.setBytes_send(fields[9]);
            v.setStatus(fields[10]);
            context.write(k, v);
        }

    }
}

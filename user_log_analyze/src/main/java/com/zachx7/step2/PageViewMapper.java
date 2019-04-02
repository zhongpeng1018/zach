package com.zachx7.step2;

import com.zachx7.bean.PageViewsBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author zach - 吸柒
 */
public class PageViewMapper extends Mapper<LongWritable,Text,Text,PageViewsBean> {

    Text k = new Text();
    PageViewsBean v = new PageViewsBean();

    /**
     * /**
     * private boolean valid = true;// 判断数据是否合法
     private String remote_addr;// 记录客户端的ip地址
     private String remote_user;// 记录客户端用户名称,忽略属性"-"
     private String time_local;// 记录访问时间与时区
     private String request;// 记录请求的url与http协议
     private String status;// 记录请求状态；成功是200
     private String body_bytes_sent;// 记录发送给客户端文件主体内容大小
     private String http_referer;// 用来记录从那个页面链接访问过来的
     private String http_user_agent;// 记录客户浏览器的相关信息

     private String session;
     private String remote_addr;
     private String timestr;
     private String request;
     private int step;
     private String staylong;
     private String referal;
     private String useragent;
     private String bytes_send;
     private String status;
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split("\001");
        if(fields[0].equals("true")){
            v.setRemote_addr(fields[1]);
            v.setTimestr(fields[3]);
            v.setRequest(fields[4]);
            v.setReferal(fields[7]);
            v.setUseragent(fields[8]);
            v.setBytes_send(fields[6]);
            v.setStatus(fields[5]);
            k.set(fields[1]);
            context.write(k,v);
        }
    }
}

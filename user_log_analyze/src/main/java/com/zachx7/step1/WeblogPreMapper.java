package com.zachx7.step1;

import com.zachx7.bean.WebLogBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

/**
 * @author zach - 吸柒
 */
public class WeblogPreMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    Text k = new Text();
    Set<String> pages = new HashSet<String>();
    //194.237.142.21 - - [18/Sep/2013:06:49:18 +0000] "GET /wp-content/uploads/2013/07/rstudio-git3.png HTTP/1.1" 304 0 "-" "Mozilla/4.0 (compatible;)"

    private SimpleDateFormat oldTime = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.US);
    private SimpleDateFormat newTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US);


    /**
     * private boolean valid = true;// 判断数据是否合法
     * private String remote_addr;// 记录客户端的ip地址
     * private String remote_user;// 记录客户端用户名称,忽略属性"-"
     * private String time_local;// 记录访问时间与时区
     * private String request;// 记录请求的url与http协议
     * private String status;// 记录请求状态；成功是200
     * private String body_bytes_sent;// 记录发送给客户端文件主体内容大小
     * private String http_referer;// 用来记录从那个页面链接访问过来的
     * private String http_user_agent;// 记录客户浏览器的相关信息
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //定义一个集合，集合当中过滤掉我们的一些静态资源
        pages.add("/about");
        pages.add("/black-ip-list/");
        pages.add("/cassandra-clustor/");
        pages.add("/finance-rhive-repurchase/");
        pages.add("/hadoop-family-roadmap/");
        pages.add("/hadoop-hive-intro/");
        pages.add("/hadoop-zookeeper-intro/");
        pages.add("/hadoop-mahout-roadmap/");
}

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(" ");
        System.out.println(Arrays.toString(fields));
        WebLogBean webLogBean = new WebLogBean();
        if (fields.length == 12) {
            webLogBean.setRemote_addr(fields[0]);
            webLogBean.setRemote_user(fields[1]);
            String time = formatDate(fields[3].substring(1));
            if (time == null || "".equals(time)) {
                time = "-invalid_time-";
            }
            webLogBean.setTime_local(time);
            webLogBean.setRequest(fields[6]);
            webLogBean.setStatus(fields[8]);
            webLogBean.setBody_bytes_sent(fields[9]);
            webLogBean.setHttp_referer(fields[10]);
            webLogBean.setHttp_user_agent(fields[11]);
        } else if (fields.length > 12) {
            webLogBean.setRemote_addr(fields[0]);
            webLogBean.setRemote_user(fields[1]);
            String time = formatDate(fields[3].substring(1));
            if (time == null || "".equals(time)) {
                time = "-invalid_time-";
            }
            webLogBean.setTime_local(time);
            webLogBean.setRequest(fields[6]);
            webLogBean.setStatus(fields[8]);
            webLogBean.setBody_bytes_sent(fields[9]);
            webLogBean.setHttp_referer(fields[10]);
            StringBuffer sb = new StringBuffer();
            for (int i = 11; i < fields.length; i++) {
                sb.append(fields[i]);
            }
            webLogBean.setHttp_user_agent(sb.toString());
        } else {
            webLogBean = null;
        }

        if (webLogBean != null) {
            String status = webLogBean.getStatus();
            try {
                if (Integer.parseInt(status) >= 400) {
                    webLogBean.setValid(false);
                }
            } catch (NumberFormatException e) {
                webLogBean.setValid(false);
            }
            if (webLogBean.getTime_local().equals("-invalid_time-")) {
                webLogBean.setValid(false);
            }

            fileStaticResource(webLogBean,pages);

            k.set(webLogBean.toString());

            context.write(k, NullWritable.get());
        }
    }

    //解析时间
    public String formatDate(String source) {
        try {
            return newTime.format(oldTime.parse(source));
        } catch (ParseException e) {
            return null;
        }
    }

    public static void fileStaticResource(WebLogBean bean, Set<String> pages) {
        if (!pages.contains(bean.getRequest())) {
            bean.setValid(false);
        }
    }


}

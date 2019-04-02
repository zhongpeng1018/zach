package com.zachx7.step2;

import com.zachx7.bean.PageViewsBean;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author zach - 吸柒
 */
public class PageViewReducer extends Reducer<Text, PageViewsBean, Text, NullWritable> {

    Text k = new Text();

    @Override
    protected void reduce(Text key, Iterable<PageViewsBean> values, Context context) throws IOException, InterruptedException {

        List<PageViewsBean> beans = new ArrayList<PageViewsBean>();
        //深克隆
        for (PageViewsBean bean : values) {
            PageViewsBean newBean = new PageViewsBean();
            try {
                BeanUtils.copyProperties(newBean, bean);
            } catch (Exception e) {
                e.printStackTrace();
            }
            beans.add(newBean);
        }

        //通过时间进行排序
        Collections.sort(beans, new Comparator<PageViewsBean>() {
            @Override
            public int compare(PageViewsBean o1, PageViewsBean o2) {
                try {
                    Date d1 = toDate(o1.getTimestr());
                    Date d2 = toDate(o2.getTimestr());
                    if (d1 == null || d2 == null)
                        return 0;
                    return d1.compareTo(d2);
                } catch (Exception e) {
                    e.printStackTrace();
                    return 0;
                }
            }
        });

        String session = UUID.randomUUID().toString();
        int step = 1;

        System.out.println(beans.size());

        for (int i = 0; i < beans.size(); i++) {
            PageViewsBean bean = beans.get(i);

            if (beans.size() == 1) {
                bean.setSession(session);
                bean.setStep(step);
                bean.setStaylong("60");
                k.set(session + "\001" + key.toString() + "\001" + bean.getTimestr() + "\001" + bean.getRequest() + "\001" + "\001" + step + "\001" + (60) + "\001" + bean.getReferal() + "\001" + bean.getUseragent() + "\001" + bean.getBytes_send() + "\001"
                        + bean.getStatus());
                context.write(k, NullWritable.get());
                session = UUID.randomUUID().toString();
                break;
            }

            //第一次跳过
            if (i == 0) continue;

            try {
                long timeDiff = timeDiff(bean.getTimestr(), beans.get(i - 1).getTimestr());

                if (timeDiff < 30 * 60 * 1000) {
                    bean.setSession(session);
                    bean.setStep(step);
                    bean.setStaylong(String.valueOf(timeDiff));
                    k.set(session + "\001" + key.toString() + "\001" + bean.getTimestr() + "\001" + bean.getRequest() + "\001" + "\001" + step + "\001" + timeDiff + "\001" + bean.getReferal() + "\001" + bean.getUseragent() + "\001" + bean.getBytes_send() + "\001"
                            + bean.getStatus());
                    context.write(k, NullWritable.get());
                    step++;
                } else {
                    bean.setSession(session);
                    bean.setStep(step);
                    bean.setStaylong(String.valueOf(timeDiff));
                    k.set(session + "\001" + key.toString() + "\001" + beans.get(i - 1).getTimestr() + "\001" + beans.get(i - 1).getRequest() + "\001" + "\001" + step + "\001" + (60) + "\001" + beans.get(i - 1).getReferal() + "\001" + beans.get(i - 1).getUseragent() + "\001" + beans.get(i - 1).getBytes_send() + "\001"
                            + beans.get(i - 1).getStatus());
                    context.write(k, NullWritable.get());
                    step = 1;
                    session = UUID.randomUUID().toString();
                }

                if (beans.size() - 1 == i) {
                    k.set(session + "\001" + key.toString() + "\001" + bean.getTimestr() + "\001" + bean.getRequest() + "\001" + "\001" + step + "\001" + (60) + "\001" + bean.getReferal() + "\001" + bean.getUseragent() + "\001" + bean.getBytes_send() + "\001"
                            + bean.getStatus());
                    context.write(k, NullWritable.get());
                }

            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
    }

    public Date toDate(String timeStr) throws ParseException {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US);
        return df.parse(timeStr);
    }

    private long timeDiff(String time1, String time2) throws ParseException {
        Date d1 = toDate(time1);
        Date d2 = toDate(time2);
        return d1.getTime() - d2.getTime();

    }
}

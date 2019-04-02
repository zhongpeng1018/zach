package com.zachx7.step3;

import com.amazonaws.services.dynamodbv2.xspec.N;
import com.zachx7.bean.PageViewsBean;
import com.zachx7.bean.VisitBean;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * @author zach - 吸柒
 */
public class VisitReducer extends Reducer<Text, PageViewsBean,VisitBean,NullWritable> {


    VisitBean k = new VisitBean();

    @Override
    protected void reduce(Text key, Iterable<PageViewsBean> values, Context context) throws IOException, InterruptedException {

        List<PageViewsBean> beans = new ArrayList<>();

        for (PageViewsBean bean : values) {
            PageViewsBean newBean = new PageViewsBean();
            try {
                BeanUtils.copyProperties(newBean,bean);
            } catch (Exception e) {
                e.printStackTrace();
            }
            beans.add(newBean);
        }

        //根据分区排序
        Collections.sort(beans, new Comparator<PageViewsBean>() {
            @Override
            public int compare(PageViewsBean o1, PageViewsBean o2) {
                return String.valueOf(o1.getStep()).compareTo(String.valueOf(o2.getStep()));
            }
        });

        /*
    private String session;
	private String remote_addr;
	private String inTime;
	private String outTime;
	private String inPage;
	private String outPage;
	private String referal;
	private int pageVisits;
     */
        k.set(key.toString(),beans.get(0).getRemote_addr(),beans.get(0).getTimestr(),beans.get(beans.size()-1).getTimestr(),beans.get(0).getRequest(),beans.get(beans.size()-1).getRequest(),beans.get(0).getReferal(),beans.size());

        context.write(k, NullWritable.get());

    }
}

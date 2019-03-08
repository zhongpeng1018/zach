package com.zachX7;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
/**
 *
 * 简单的爬取图片小demo 
 *
 */
public class PicSpider {

    public static int num = 1;

    public static void main(String[] args) throws IOException {
		
		//一个动漫网站
        String url = "https://www.mkzhan.com/213892/774730.html";

        HttpGet httpGet = new HttpGet(url);

        CloseableHttpClient httpClient = HttpClients.createDefault();

        CloseableHttpResponse response = httpClient.execute(httpGet);

        String html = EntityUtils.toString(response.getEntity(), Charset.forName("utf-8"));

        //System.out.println(html);
        Document doc = Jsoup.parse(html);

        Elements elements = doc.select("div[data-page_id] img");

        for (Element element : elements) {
            String imgUrl = element.attr("data-src");
            int i = imgUrl.lastIndexOf("!");
            imgUrl = imgUrl.substring(0, i);
            System.out.println(imgUrl);
            download(imgUrl);
        }

    }

    public static void download(String url) throws IOException {

        HttpGet httpGet = new HttpGet(url);

        CloseableHttpClient httpClient = HttpClients.createDefault();

        CloseableHttpResponse response = httpClient.execute(httpGet);

        byte[] bytes = EntityUtils.toByteArray(response.getEntity());

		//指定目录
        File file = new File("C:\\Users\\zhongpeng\\Desktop\\img");

        if(!file.exists()){
            file.mkdirs();
        }

        String imgName = "第"+num+"页" + url.substring(url.lastIndexOf("."));

        BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(new File(file,imgName)));

        bos.write(bytes);

        bos.flush();

        bos.close();

        num++;

    }

}

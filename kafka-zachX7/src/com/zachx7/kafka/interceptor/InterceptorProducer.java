package com.zachx7.kafka.interceptor;

import java.util.ArrayList;
import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class InterceptorProducer {

	public static void main(String[] args) {
		Properties props = new Properties();
		// 配置kafka服务器地址
		props.put("bootstrap.servers", "hadoop102:9092");
		// 配置回应
		props.put("acks", "all");
		// 配置重连次数
		props.put("retries", 0);
		// 批处理（暂时没啥用）
		props.put("batch.size", 16384);
		// 发送间隔
		props.put("linger.ms", 1);
		// 缓冲内存大小
		props.put("buffer.memory", 33554432);
		// key value 序列化
		props.put("key.serializer",
				"org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer",
				"org.apache.kafka.common.serialization.StringSerializer");
		//设置拦截链
		ArrayList<String> interceptors = new ArrayList<>();
		interceptors.add("com.zachx7.kafka.interceptor.TimeInterceptor");
		interceptors.add("com.zachx7.kafka.interceptor.CountInterceptor");
		props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, interceptors);
		
		Producer<String, String> producer = new KafkaProducer<>(props);
		for (int i = 0; i < 10; i++)
			producer.send(
					new ProducerRecord<String, String>("zach1", Integer
							.toString(i), "zach-吸柒" + "(" + i + ")"),
					new Callback() {

						@Override
						public void onCompletion(RecordMetadata metadata,
								Exception exception) {
							if (metadata != null) {
								System.err.println(metadata.timestamp() + "---"
										+ metadata.partition());
							}

						}
					});

		producer.close();
	}
	
}

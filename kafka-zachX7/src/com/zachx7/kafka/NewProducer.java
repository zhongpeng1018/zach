package com.zachx7.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class NewProducer {

	public static void main(String[] args) {
		
		Properties props = new Properties();
		//配置kafka服务器地址
		props.put("bootstrap.servers", "hadoop102:9092");
		//配置回应
		props.put("acks", "all");
		//配置重连次数
		props.put("retries", 0);
		//批处理（暂时没啥用）
		props.put("batch.size", 16384);
		//发送间隔 
		props.put("linger.ms", 1);
		// 缓冲内存大小
		props.put("buffer.memory", 33554432);
		// key value 序列化
		props.put("key.serializer",
				"org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer",
				"org.apache.kafka.common.serialization.StringSerializer");

		Producer<String, String> producer = new KafkaProducer<>(props);
		for (int i = 0; i < 100; i++)
			producer.send(new ProducerRecord<String, String>("zach1",
					Integer.toString(i), "zach-吸柒"+"("+i+")"));

		producer.close();

	}

}

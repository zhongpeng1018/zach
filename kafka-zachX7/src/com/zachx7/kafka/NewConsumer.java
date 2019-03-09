package com.zachx7.kafka;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class NewConsumer {

	@SuppressWarnings("resource")
	public static void main(String[] args) {
		// 配置信息
		Properties props = new Properties();

		// 配置kafka服务器地址
		props.put("bootstrap.servers", "hadoop102:9092");

		// 配置消费者组ID
		props.put("group.id", "x7");

		// 是否开启自动提交（offset）
		props.put("enable.auto.commit", "true");

		// 多久提交一次
		props.put("auto.commit.interval.ms", "1000");

		// 序列化
		props.put("key.deserializer",
				"org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer",
				"org.apache.kafka.common.serialization.StringDeserializer");

		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

		consumer.subscribe(Arrays.asList("zach1", "zach2"));
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(100);

			for (ConsumerRecord<String, String> record : records) {
				System.out.printf("offset = %d, key = %s, value = %s%n",
						record.offset(), record.key(), record.value());
			}
		}
	}
}

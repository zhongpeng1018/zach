package com.zachx7.kafka;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class NewConsumer {

	@SuppressWarnings("resource")
	public static void main(String[] args) {
		// ������Ϣ
		Properties props = new Properties();

		// ����kafka��������ַ
		props.put("bootstrap.servers", "hadoop102:9092");

		// ������������ID
		props.put("group.id", "x7");

		// �Ƿ����Զ��ύ��offset��
		props.put("enable.auto.commit", "true");

		// ����ύһ��
		props.put("auto.commit.interval.ms", "1000");

		// ���л�
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

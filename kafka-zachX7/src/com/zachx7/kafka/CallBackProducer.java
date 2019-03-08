package com.zachx7.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class CallBackProducer {

	public static void main(String[] args) {

		Properties props = new Properties();
		// ����kafka��������ַ
		props.put("bootstrap.servers", "hadoop102:9092");
		// ���û�Ӧ
		props.put("acks", "all");
		// ������������
		props.put("retries", 0);
		// ����������ʱûɶ�ã�
		props.put("batch.size", 16384);
		// ���ͼ��
		props.put("linger.ms", 1);
		// �����ڴ��С
		props.put("buffer.memory", 33554432);
		// key value ���л�
		props.put("key.serializer",
				"org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer",
				"org.apache.kafka.common.serialization.StringSerializer");

		Producer<String, String> producer = new KafkaProducer<>(props);
		for (int i = 0; i < 100; i++)
			producer.send(
					new ProducerRecord<String, String>("zach2", Integer
							.toString(i), "zach-����" + "(" + i + ")"),
					new Callback() {

						@Override
						public void onCompletion(RecordMetadata metadata,
								Exception exception) {
							if (metadata != null) {
								System.err.println(metadata.timestamp() + "---" + metadata.partition());
							}

						}
					});

		producer.close();

	}

}
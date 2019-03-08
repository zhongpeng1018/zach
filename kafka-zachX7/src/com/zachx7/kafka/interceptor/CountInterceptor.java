package com.zachx7.kafka.interceptor;

import java.util.Map;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class CountInterceptor implements ProducerInterceptor<String, String> {

	private int successCount = 0;

	private int errorCount = 0;

	@Override
	public void configure(Map<String, ?> configs) {
		// TODO Auto-generated method stub

	}

	@Override
	public ProducerRecord<String, String> onSend(
			ProducerRecord<String, String> record) {
		// TODO Auto-generated method stub
		return record;
	}

	@Override
	public void onAcknowledgement(RecordMetadata metadata, Exception exception) {

		if (exception == null) {
			successCount++;
		} else {
			errorCount++;
		}

	}

	@Override
	public void close() {
		System.err.println("success :" + successCount);
		System.err.println("error :" + errorCount);
	}

}

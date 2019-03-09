package com.zachx7.kafka.stream;

import java.util.Properties;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.TopologyBuilder;

public class Application {

	public static void main(String[] args) {
		// 1、配置属性
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "logFilter"); // 配置程序ID
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092"); // 配置KAFKA服务器地址
		// 拓扑构建
		TopologyBuilder builder = new TopologyBuilder()
				.addSource("source", "zach1")
				.addProcessor("processor",
						new ProcessorSupplier<byte[], byte[]>() {

							@Override
							public Processor<byte[], byte[]> get() {
								return new LogProcessor();
							}
						}, "source").addSink("sink", "zach2", "processor");

		KafkaStreams kafkaStreams = new KafkaStreams(builder, props);
		kafkaStreams.start();

	}

}

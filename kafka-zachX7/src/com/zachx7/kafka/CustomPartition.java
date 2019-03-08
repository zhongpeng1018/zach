package com.zachx7.kafka;

import java.util.Map;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

/**
 * 最简单的kafka自定义分区 
 * @author zach - 吸柒
 *
 */
public class CustomPartition implements Partitioner{

	/**
	 * 初始化配置
	 */
	@Override
	public void configure(Map<String, ?> configs) {
		// TODO Auto-generated method stub
		
	}

	/**
	 * 具体分区逻辑
	 */
	@Override
	public int partition(String topic, Object key, byte[] keyBytes,
			Object value, byte[] valueBytes, Cluster cluster) {
		//这里就简单的固定分区
		return 2;
	}

	/**
	 * 关闭资源
	 */
	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	
	
}

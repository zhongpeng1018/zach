package com.zachx7.kafka;

import java.util.Map;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

/**
 * ��򵥵�kafka�Զ������ 
 * @author zach - ����
 *
 */
public class CustomPartition implements Partitioner{

	/**
	 * ��ʼ������
	 */
	@Override
	public void configure(Map<String, ?> configs) {
		// TODO Auto-generated method stub
		
	}

	/**
	 * ��������߼�
	 */
	@Override
	public int partition(String topic, Object key, byte[] keyBytes,
			Object value, byte[] valueBytes, Cluster cluster) {
		//����ͼ򵥵Ĺ̶�����
		return 2;
	}

	/**
	 * �ر���Դ
	 */
	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	
	
}

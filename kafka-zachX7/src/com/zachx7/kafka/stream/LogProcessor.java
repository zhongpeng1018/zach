package com.zachx7.kafka.stream;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

public class LogProcessor implements Processor<byte[], byte[]> {

	private ProcessorContext context;
	
	@Override
	public void init(ProcessorContext context) {
		this.context = context;
	}

	@Override
	public void process(byte[] key, byte[] value) {
		
		String input = new String(value);
		
		if(input.contains(">>>")){
			String[] fields = input.split(">>>");
			context.forward(key, fields[1].getBytes());
		}else{
			context.forward(key, value);
		}

	}

	@Override
	public void punctuate(long timestamp) {
		// TODO Auto-generated method stub

	}

	@Override
	public void close() {
		// TODO Auto-generated method stub

	}

}

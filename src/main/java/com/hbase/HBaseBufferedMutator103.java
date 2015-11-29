package com.hbase;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import eventsimulator.EventSimulator;

public class HBaseBufferedMutator103 implements Runnable {
	private static final Log LOG = LogFactory.getLog(HBaseBufferedMutator103.class);

	/** The identifier for the application table. */
	private static final TableName TABLE_NAME = TableName.valueOf("netiq:sentinel-events");
	/** The name of the column family used by the application. */
	private static final byte[] CF = Bytes.toBytes("cf1");

	private static final AtomicInteger TASK_COUNT = new AtomicInteger(10000000);
	private static final int POOL_SIZE = 5;
	
	private static final ExecutorService workerPool = Executors.newFixedThreadPool(POOL_SIZE);

	public static void main(String[] args) throws Exception {
		long start = System.currentTimeMillis();
		
		for(int i=0;i<POOL_SIZE;i++){
			workerPool.submit(new HBaseBufferedMutator103());
		}
		
		workerPool.shutdown();
		workerPool.awaitTermination(1, TimeUnit.MINUTES);
		
		long end = System.currentTimeMillis();
		System.out.println("Took " + (end - start) / 1000 + " seconds for processing " + TASK_COUNT + " records");
	}
	
	public void run() {
		Configuration configuration = HBaseConfiguration.create();
		BufferedMutatorParams params = new BufferedMutatorParams(TABLE_NAME);
		params.writeBufferSize(2097152*10);
		
		try{
			Connection conn = ConnectionFactory.createConnection(configuration);
			BufferedMutator mutator = conn.getBufferedMutator(params);		
			
			while(TASK_COUNT.decrementAndGet()>0){
				Map<String, String> event = EventSimulator.getEvent(0);
				Put p = getPutForEvent(event,true);
				mutator.mutate(p);
				//mutator.flush();
			}
	
			mutator.close();
			conn.close();
		}catch(Exception e){
			e.printStackTrace();
		}		
	}

	public static Put getPutForEvent(Map<String, String> event,boolean block) {
		UUID uuid = UUID.randomUUID();
		//Put put = new Put(new StringBuilder(uuid.toString()).reverse().toString().getBytes());
		Put put = new Put(new StringBuilder(uuid.toString()).toString().getBytes());
		
		put.setDurability(Durability.SKIP_WAL);

		
		if(block){
			put.addColumn(Bytes.toBytes("evt"), "data".getBytes(), event.toString().getBytes());
		}else{		
			for (Map.Entry<String, String> entry : event.entrySet()) {
				if (entry.getValue() == null || entry.getValue().isEmpty() || entry.getKey() == null) {
					System.out.println(event);
					continue;
				}
	
				put.addColumn(Bytes.toBytes("evt"), entry.getKey().getBytes(), entry.getValue().getBytes());
			}
		}
		
		return put;
	}
}
package com.hbase;

import java.util.Map;
import java.util.UUID;

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

public class HBaseBufferedMutator101 {
	private static final Log LOG = LogFactory.getLog(HBaseBufferedMutator101.class);

	/** The identifier for the application table. */
	private static final TableName TABLE_NAME = TableName.valueOf("netiq:sentinel-events");
	/** The name of the column family used by the application. */
	private static final byte[] CF = Bytes.toBytes("cf1");

	private static final int TASK_COUNT = 1000000;

	public static void main(String[] args) throws Exception {
		Configuration configuration = HBaseConfiguration.create();

		BufferedMutatorParams params = new BufferedMutatorParams(TABLE_NAME);
		params.writeBufferSize(20971520);
		Connection conn = ConnectionFactory.createConnection(configuration);

		long start = System.currentTimeMillis();
		BufferedMutator mutator = conn.getBufferedMutator(params);		
		
		for(int i=0;i<TASK_COUNT;i++){
			Map<String, String> event = EventSimulator.getEvent(0);
			Put p = getPutForEvent(event,true);
			mutator.mutate(p);
			//mutator.flush();
		}

		mutator.close();
		conn.close();
		long end = System.currentTimeMillis();
		System.out.println("Took " + (end - start) / 1000 + " seconds for processing " + TASK_COUNT + " records");
	}

	public static Put getPutForEvent(Map<String, String> event,boolean block) {
		UUID uuid = UUID.randomUUID();
		//Put put = new Put(new StringBuilder(uuid.toString()).reverse().toString().getBytes());
		Put put = new Put(new StringBuilder(uuid.toString()).toString().getBytes());
		
		//put.setDurability(Durability.SKIP_WAL);

		
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
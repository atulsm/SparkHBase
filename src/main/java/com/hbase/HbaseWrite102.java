package com.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import eventsimulator.EventSimulator;

public class HbaseWrite102 extends Configured implements Tool {

	/** The identifier for the application table. */
	private static final TableName TABLE_NAME = TableName.valueOf("netiq:sentinel-events");
	/** The name of the column family used by the application. */
	private static final byte[] CF = Bytes.toBytes("cf1");

	ExecutorService workerPool = Executors.newFixedThreadPool(5);

	public int run(String[] argv) throws IOException {
		Configuration conf = new Configuration();// HBaseConfiguration.create();
		conf.addResource("hbase-site.xml");
		conf.addResource("core-site.xml");
		conf.addResource("hdfs-site.xml");

		/**
		 * Connection to the cluster. A single connection shared by all
		 * application threads.
		 */
		Connection connection = null;
		Table table = null;
		try {
			// establish the connection to the cluster.
			connection = ConnectionFactory.createConnection(conf);
			// retrieve a handle to the target table.

			/**
			 * A lightweight handle to a specific table. Used from a single
			 * thread.
			 */
			table = connection.getTable(TABLE_NAME);

			long tosend = 10000;
			long batchsize = 5000;
			long start = System.currentTimeMillis();

			long delta = System.currentTimeMillis();

			List<Put> puts = new ArrayList<>();
			for (int i = 0; i < tosend; i++) {
				Map<String, String> event = EventSimulator.getEvent(0);

				// describe the data we want to write.
				Put p = getPutForEvent(event);
				// Put p = getBlobPutForEvent(event);
				puts.add(p);

				if (i % batchsize == 0) {
					workerPool.submit(new BatchRunner(table, puts));
					puts.clear();
				}

				// send the data.
				// table.put(p);
			}

			try {
				workerPool.shutdown();
				workerPool.awaitTermination(10, TimeUnit.MINUTES);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

			long end = System.currentTimeMillis();

			System.out.println("Took " + (end - start) / 1000 + " seconds for processing " + tosend + " records");

		} finally {
			// close everything down
			if (table != null)
				table.close();
			if (connection != null)
				connection.close();
		}
		return 0;
	}

	public Put getPutForEvent(Map<String, String> event) {
		UUID uuid = UUID.randomUUID();
		Put put = new Put(uuid.toString().getBytes());

		for (Map.Entry<String, String> entry : event.entrySet()) {
			if (entry.getValue() == null || entry.getValue().isEmpty())
				continue;

			put.addColumn(Bytes.toBytes("evt"), entry.getKey().getBytes(), entry.getValue().getBytes());
		}
		return put;
	}

	public Put getBlobPutForEvent(Map<String, String> event) {
		UUID uuid = UUID.randomUUID();
		Put put = new Put(uuid.toString().getBytes());
		put.addColumn(Bytes.toBytes("evt"), Bytes.toBytes("data"), event.toString().getBytes());

		return put;
	}

	private static class BatchRunner implements Runnable {
		Table table;
		List<Put> puts;

		public BatchRunner(Table table, List<Put> puts) {
			this.table = table;
			this.puts = new ArrayList<>(puts);
		}

		public void run() {
			long start = System.currentTimeMillis();
			try {
				table.put(puts);
			} catch (IOException e) {
				e.printStackTrace();
			}
			System.out.println(new Date() + " Took " + (puts.size())/((System.currentTimeMillis()-start)/1000));
		}
	}

	public static void main(String[] argv) throws Exception {
		int ret = ToolRunner.run(new HbaseWrite102(), argv);
		System.exit(ret);
	}
}

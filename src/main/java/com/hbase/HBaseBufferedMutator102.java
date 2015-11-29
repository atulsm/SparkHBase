package com.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.util.Bytes;

import eventsimulator.EventSimulator;


// cc BufferedMutatorExample Shows the use of the client side write buffer
public class HBaseBufferedMutator102 {
	  private static final Log LOG = LogFactory.getLog(HBaseBufferedMutator102.class);

	  /** The identifier for the application table. */
	  private static final TableName TABLE_NAME = TableName.valueOf("netiq:sentinel-events");
	  /** The name of the column family used by the application. */
	  private static final byte[] CF = Bytes.toBytes("cf1");
	

  // vv BufferedMutatorExample
  private static final int POOL_SIZE = 10;
  private static final int TASK_COUNT = 100000;

  public static void main(String[] args) throws Exception {
    Configuration configuration = HBaseConfiguration.create();

    // vv BufferedMutatorExample
    BufferedMutator.ExceptionListener listener =
      new BufferedMutator.ExceptionListener() { 
      @Override
      public void onException(RetriesExhaustedWithDetailsException e,
        BufferedMutator mutator) {
        for (int i = 0; i < e.getNumExceptions(); i++) { 
          LOG.info("Failed to sent put: " + e.getRow(i));
        }
      }
    };
    
    BufferedMutatorParams params = new BufferedMutatorParams(TABLE_NAME).listener(listener);
    params.writeBufferSize(20971520*2);

    long start = System.currentTimeMillis();    

    try (
      Connection conn = ConnectionFactory.createConnection(configuration);
      BufferedMutator mutator = conn.getBufferedMutator(params)
    ) {
      ExecutorService workerPool = Executors.newFixedThreadPool(POOL_SIZE); // co BufferedMutatorExample-06-Pool Create a worker pool to update the shared mutator in parallel.
      List<Future<Void>> futures = new ArrayList<>(TASK_COUNT);

      for (int i = 0; i < TASK_COUNT; i++) { // co BufferedMutatorExample-07-Threads Start all the workers up.
        futures.add(workerPool.submit(new Callable<Void>() {
          @Override
          public Void call() throws Exception {
	    	  Map<String, String> event = EventSimulator.getEvent(0);	      	     

        	Put p = getPutForEvent(event);
            mutator.mutate(p); 

            return null;
          }
        }));
      }

      for (Future<Void> f : futures) {
        f.get(5, TimeUnit.MINUTES); // co BufferedMutatorExample-09-Shutdown Wait for workers and shut down the pool.
      }
      
      workerPool.shutdown();
      workerPool.awaitTermination(1, TimeUnit.MINUTES);
      
    } catch (IOException e) { // co BufferedMutatorExample-10-ImplicitClose The try-with-resource construct ensures that first the mutator, and then the connection are closed. This could trigger exceptions and call the custom listener.
      LOG.info("Exception while creating or freeing resources", e);
    }
    
    long end = System.currentTimeMillis();    
    System.out.println("Took " + (end-start)/1000 + " seconds for processing " + TASK_COUNT+ " records");
  }
  
  public static Put getPutForEvent(Map<String,String> event) {
	  UUID uuid = UUID.randomUUID();
	  Put put = new Put(uuid.toString().getBytes());
        
        for(Map.Entry<String, String> entry:event.entrySet())
        {
              if(entry.getValue() == null || entry.getValue().isEmpty() || entry.getKey() == null){
            	  System.out.println(event); 
            	  continue;
              }
                  
              
              put.addColumn(Bytes.toBytes("evt"), entry.getKey().getBytes(), entry.getValue().getBytes());
        }
      return put;
    }
}
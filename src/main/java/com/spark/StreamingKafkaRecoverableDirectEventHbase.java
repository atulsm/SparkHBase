package com.spark;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaStreamingContextFactory;
import org.apache.spark.streaming.kafka.KafkaUtils;

import eventsimulator.ObjectSerializer;
import kafka.serializer.DefaultDecoder;
import scala.Tuple2;

/**
 * read from a folder. Keep adding new file with data into the folder specified.
 */
public final class StreamingKafkaRecoverableDirectEventHbase {
	private static String checkpointDir = "hdfs://idcdvstl233:8020/tmp/StreamingKafkaRecoverableDirectEventHbase";
	private static boolean streamCheckPoint = false;
	private static int duration = 10;
	private static Configuration configuration = null;
	private static Job newAPIJobConfiguration1 = null;
	
	public static void main(String[] args) throws IOException {
		Logger.getLogger("org").setLevel(Level.WARN);
		Logger.getLogger("akka").setLevel(Level.WARN);		
		
		configuration = HBaseConfiguration.create();
		configuration.set(TableOutputFormat.OUTPUT_TABLE, "netiq:sentinel-events");
		
		// new Hadoop API configuration
		newAPIJobConfiguration1 = Job.getInstance(configuration);
		newAPIJobConfiguration1.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, "netiq:sentinel-events");
		newAPIJobConfiguration1.setOutputFormatClass(org.apache.hadoop.hbase.mapreduce.TableOutputFormat.class);
		
		//configuration.setOutputFormatClass(TableOutputFormat.class);
		
		if (args.length > 0) {
			try {
				duration = Integer.parseInt(args[0]);
				System.out.println("duration changed to " + duration);
			} catch (Exception e) {
				System.out.println("Duration reset to defaults");
			}
			
			if(args.length>1){
				try{
					streamCheckPoint = Boolean.getBoolean(args[1]);
					System.out.println("streamCheckPoint changed to " + streamCheckPoint);
				}catch (Exception e) {
					System.out.println("streamCheckPoint reset to defaults");
				}
			}
			
			if(args.length>2){
				checkpointDir = args[2];
				System.out.println("checkpointDir changed to " + checkpointDir);
			}			
		}
		
		
		JavaStreamingContext ssc = JavaStreamingContext.getOrCreate(checkpointDir,new JavaStreamingContextFactory() {			
			@Override
			public JavaStreamingContext create() {
				return createContext(checkpointDir, streamCheckPoint, duration);
			}
		});
		
		ssc.start();
		ssc.awaitTermination();
	}
	
	
	
	public static JavaStreamingContext createContext(String checkpointDirectory, boolean streamCheckPoint, int duration) {
		SparkConf sparkConf = new SparkConf().setAppName("StreamingKafkaRecoverableDirectEvent");

		// Only for running from eclipse
		if (System.getProperty("dev") != null)
			sparkConf.setJars(new String[] { "target\\SparkHbase-0.0.1-SNAPSHOT.jar" });

		sparkConf.set("spark.executor.memory", "4G");

		// for elasticsearch
		sparkConf.set("es.nodes", "10.204.102.200");
		sparkConf.set("es.index.auto.create", "true");

		final int streamingDuration = duration;

		JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(streamingDuration));		
		ssc.checkpoint(checkpointDir);
		
		HashSet<String> topicsSet = new HashSet<String>();
		topicsSet.add("loadtest");

		HashMap<String, String> kafkaParams = new HashMap<String, String>();
		kafkaParams.put("metadata.broker.list", "10.204.100.180:19092");

		JavaPairInputDStream<byte[], byte[]> messages = KafkaUtils.createDirectStream(ssc, byte[].class, byte[].class,
				DefaultDecoder.class, DefaultDecoder.class, kafkaParams, topicsSet);
		
		if(streamCheckPoint){
			messages.checkpoint(Durations.minutes(1));
		}
		
		JavaDStream<Map<String, String>> lines = messages
				.map(new Function<Tuple2<byte[], byte[]>, Map<String, String>>() {
					@Override
					public Map<String, String> call(Tuple2<byte[], byte[]> tuple2) {
						Map<String, String> ret = (Map<String, String>) ObjectSerializer.getEvent(tuple2._2());

						process(ret);
						return ret;
					}

					private void process(Map<String, String> ret) {
						ret.put("obscountry", "US");
					}
				});
		
		/**
		JavaDStream<Map<String, String>> persistlines = lines.cache();		
		
		JavaPairDStream<String, Map<String, String>> hbaseEvent = persistlines.mapToPair(new PairFunction<Map<String,String>, String, Map<String,String>>() {
			@Override
			public Tuple2<String, Map<String,String>> call(Map<String, String> event) throws Exception {
				return new Tuple2<String, Map<String,String>>(event.get("id"), event);
			}
		});
		
		
		hbaseEvent.saveAsHadoopFiles(prefix, suffix);
		*/
		
		

		lines.foreachRDD(new Function<JavaRDD<Map<String, String>>, Void>() {
			@Override
			public Void call(JavaRDD<Map<String, String>> rdd) throws Exception {
				long start = System.currentTimeMillis();

				try{
					//return saveRDDtoES(rdd);
					return saveRDDtoHBase(rdd);
				}finally{
					long processTime = System.currentTimeMillis() - start;
					System.out.println(new Date() + "  Stats: rddProcessTime:" + processTime);	
				}
			}
			
			private Void saveRDDtoHBase(JavaRDD<Map<String, String>> rdd) {
				JavaPairRDD<ImmutableBytesWritable, Put> hbasePuts = rdd.mapToPair(new PairFunction<Map<String, String>, ImmutableBytesWritable, Put>() {
					@Override
					public Tuple2<ImmutableBytesWritable, Put> call(Map<String, String> event) throws Exception {					        
						Put put = getHbasePutForEvent(event, true);			     
					    return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(), put);     
					}
				});
				
				// save to HBase- Spark built-in API method
			    hbasePuts.saveAsNewAPIHadoopDataset(newAPIJobConfiguration1.getConfiguration());
			
				return null;
			}			

			private Void saveRDDtoES(JavaRDD<Map<String, String>> rdd) {
				try {
					Map<String, String> idmap = new HashMap<>();
					idmap.put("es.mapping.id", "id");						
					//JavaEsSpark.saveToEs(rdd, "events/event",idmap);											
				} catch (Exception es) {
					es.printStackTrace();
				}
				return null;
			}
		});
		
		
		return ssc;
	}
	
	public static Put getHbasePutForEvent(Map<String, String> event,boolean singleColumn) {
		UUID uuid = UUID.randomUUID();
		//Put put = new Put(new StringBuilder(uuid.toString()).reverse().toString().getBytes());
		Put put = new Put(new StringBuilder(uuid.toString()).toString().getBytes());
		
		//put.setDurability(Durability.SKIP_WAL);

		
		if(singleColumn){
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

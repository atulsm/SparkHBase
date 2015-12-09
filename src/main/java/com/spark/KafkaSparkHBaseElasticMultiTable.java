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
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaStreamingContextFactory;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

import eventsimulator.ObjectSerializer;
import kafka.serializer.DefaultDecoder;
import scala.Tuple2;

/**
 * This class does it all. :)
 *  Reads data from Kafka (Using direct stream)
 *  Use checkpoint to make sure that spark job failiures are handle properly
 *  After reading data from Kafka RDD's are cached.
 *    A copy of cached RDD is written to HBase
 *    Another copy is written to ElasticSearch using ElasticSearchHadoop library
 *    Wtites to multiple tables
 * 
 * 
 * @author atulsoman
 */
public final class KafkaSparkHBaseElasticMultiTable {
	private static String checkpointDir = "hdfs://idcdvstl233:8020/tmp/KafkaSparkHBaseElasticMultiTable";
	
	private static CommandLineConfig config = null;
	private static Map<String, Configuration> tableConfMap = new HashMap<>();
		
	public static void main(String[] args) throws IOException {
		Logger.getLogger("org").setLevel(Level.WARN);
		Logger.getLogger("akka").setLevel(Level.WARN);		
		config = new CommandLineConfig(args);							
		
		JavaStreamingContext ssc = JavaStreamingContext.getOrCreate(checkpointDir,new JavaStreamingContextFactory() {			
			@Override
			public JavaStreamingContext create() {
				return createContext(checkpointDir, config.duration);
			}
		});
		
		ssc.start();
		ssc.awaitTermination();
	}

	private static synchronized Configuration getHbaseTableConfiguration(String tableName) {
		Configuration conf = tableConfMap.get(tableName);
		if(conf == null){
			// new Hadoop API configuration for hbase write
			Job newAPIJobConfiguration1=null;
			try {
				newAPIJobConfiguration1 = Job.getInstance(HBaseConfiguration.create());
			} catch (IOException e) {
				e.printStackTrace();
				return null;
			}
			newAPIJobConfiguration1.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, tableName);
			newAPIJobConfiguration1.setOutputFormatClass(org.apache.hadoop.hbase.mapreduce.TableOutputFormat.class);
			conf = newAPIJobConfiguration1.getConfiguration();
			tableConfMap.put(tableName, conf);
		}
		return conf;
	}
	
	
	
	public static JavaStreamingContext createContext(String checkpointDirectory, int duration) {
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

		lines.foreachRDD(new Function<JavaRDD<Map<String, String>>, Void>() {
			@Override
			public Void call(JavaRDD<Map<String, String>> rdd) throws Exception {				
				long start = System.currentTimeMillis();
				JavaRDD<Map<String, String>> cachedRdd = rdd.persist(StorageLevel.MEMORY_ONLY());//rdd.cache();
				
				try{
					if(config.parallelSave){
						saveInParallel(cachedRdd);
					}else{
						saveRDDtoHBase(cachedRdd);
						saveRDDtoES(cachedRdd);
					}
					
					cachedRdd.unpersist();
				}finally{
					long esSaveTime = System.currentTimeMillis() - start;
					System.out.println(new Date() + "  Stats: TotalTime: " + esSaveTime);	
				}
				
				return null;
			}

			private void saveInParallel(JavaRDD<Map<String, String>> cachedRdd) throws InterruptedException {
				Thread job1 = new Thread(){
					public void run() {
						saveRDDtoHBase(cachedRdd);
					};
				};
				
				Thread job2 = new Thread(){
					public void run() {
						saveRDDtoES(cachedRdd);
					};
				};
				
				job1.start();
				job2.start();
				
				job1.join();
				job2.join();
			}
			
			private Void saveRDDtoHBase(JavaRDD<Map<String, String>> rdd) {
				if(!config.sendToHbase)
					return null;
				
				long start = System.currentTimeMillis();
				JavaPairRDD<ImmutableBytesWritable, Put> hbasePuts = rdd.mapToPair(new PairFunction<Map<String, String>, ImmutableBytesWritable, Put>() {
					@Override
					public Tuple2<ImmutableBytesWritable, Put> call(Map<String, String> event) throws Exception {					        
						Put put = getPutForEvent(event, true);			     
					    return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(), put);     
					}
				});				
				// save to HBase- Spark built-in API method
			    hbasePuts.saveAsNewAPIHadoopDataset(getHbaseTableConfiguration("netiq:sentinel-events-default"));
			
			    long hbaseSaveTime = System.currentTimeMillis() - start;
				System.out.println(new Date() + "  Stats: HbaseSave: " + hbaseSaveTime);				    
				return null;
			}			

			private Void saveRDDtoES(JavaRDD<Map<String, String>> rdd) {
				if(!config.sendToES)
					return null;
				
				long start = System.currentTimeMillis();
				try {
					Map<String, String> idmap = new HashMap<>();
					idmap.put("es.mapping.id", "id");						
					JavaEsSpark.saveToEs(rdd, "events/event",idmap);											
				} catch (Exception es) {
					es.printStackTrace();
				}				
			    long esSaveTime = System.currentTimeMillis() - start;
				System.out.println(new Date() + "  Stats: ElasticSearchSave: " + esSaveTime);	
				
				return null;
			}
		});
		
		
		return ssc;
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

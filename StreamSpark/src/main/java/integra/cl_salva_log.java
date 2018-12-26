package integra;

import static org.apache.spark.sql.functions.col;

import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

public class cl_salva_log {
	
	private static int gv_submit = 1; //1=Cluster 
	
	final static String gv_table = "LOG";//"JSON00";
	
	final static String gv_devices = "BRO_DEVICES";
	
	final static String gv_zkurl = "localhost:2181";
	
	final static String gc_conn = "conn";
	final static String gc_dns  = "dns";
	final static String gc_http = "http";
	final static String gc_ssh  = "ssh";
	final static String gc_dhcp = "dhcp";
	
	final static String gc_devices = "known_devices";
	
	final static Collection<String> gv_topics = Arrays.asList("BroLog");
	
	static cl_salva_log gv_salva_log;
		
	public static void main(String[] args) throws InterruptedException {
		
		gv_salva_log = new cl_salva_log();
		
		gv_salva_log.m_start();
				
	}
	
	public void m_start() throws InterruptedException {
		
		Map<String, Object> lv_kafkaParams = new HashMap<String, Object>();
		
		lv_kafkaParams = gv_salva_log.m_conecta_kafka();
		
		gv_salva_log.m_consome_kafka(lv_kafkaParams);
		
	}
	
	public Map<String, Object> m_conecta_kafka() {
		
		// Configure Spark to connect to Kafka running on local machine
		Map<String, Object> lv_params = new HashMap<String, Object>();

		lv_params.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

		lv_params.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringDeserializer");

		lv_params.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringDeserializer");

		lv_params.put(ConsumerConfig.GROUP_ID_CONFIG, "group1");		

		lv_params.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

		lv_params.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);			
		
		return lv_params;
		
	}
	
	public static void m_consome_kafka(Map<String, Object> lv_kafka) throws InterruptedException {
		
		SparkConf lv_conf;
		
		if(gv_submit == 0) {			
			lv_conf = new SparkConf().setMaster("local[2]").setAppName("BroLogConn");
		}else {
			lv_conf = new SparkConf().setAppName("BroLogConn");//se for executar no submit
		}
		
		// Read messages in batch of 30 seconds
		JavaStreamingContext lv_jssc = new JavaStreamingContext(lv_conf, Durations.seconds(3));// Durations.milliseconds(10));
																				
		// Disable INFO messages-> 
		Logger.getRootLogger().setLevel(Level.ERROR);

		// Start reading messages from Kafka and get DStream
		final JavaInputDStream<ConsumerRecord<String, String>> lv_stream = KafkaUtils.createDirectStream(lv_jssc,
				LocationStrategies.PreferConsistent(),
				ConsumerStrategies.<String, String>Subscribe(gv_topics, lv_kafka));

		// Read value of each message from Kafka and return it
		JavaDStream<String> lv_lines = lv_stream.map(new Function<ConsumerRecord<String, String>, String>() {
			public String call(ConsumerRecord<String, String> lv_kafkaRecord) throws Exception {
				return lv_kafkaRecord.value();
			}
		});

		lv_lines.foreachRDD((rdd, time) -> {
			
			Date lv_time = new Date();
			
			long lv_stamp = lv_time.getTime();				
			
			System.out.println("Dados:Do RDD = " + rdd.count() +"\t TIME: "+lv_stamp);

			SparkSession lv_sess = SparkSession.builder().config(rdd.context().getConf()).getOrCreate();
			
			List<String> lv_rdd = rdd.collect();
			
			Dataset<String> lv_json = lv_sess.createDataset(lv_rdd, Encoders.STRING());
			
			Dataset<Row> lv_data = lv_sess.read().json(lv_json);
			
			m_save_log(lv_data, gc_conn, lv_stamp);
			
			m_save_log(lv_data, gc_dns, lv_stamp);
			
		    m_save_log(lv_data, gc_http, lv_stamp);
		    
		    m_save_log(lv_data, gc_ssh, lv_stamp);
		    
		    m_save_log(lv_data, gc_dhcp, lv_stamp);
		    
		    m_save_devices(lv_data, gc_devices, lv_stamp);
			
			//lv_data.printSchema();
			
			//lv_data.show();											
						
		});
		
		//lv_lines.print();
		lv_jssc.start();
		lv_jssc.awaitTermination();
				
	}
	
	public static void m_save_log(Dataset<Row> lv_data, String lv_tipo, long lv_stamp) {
		
		try {						
			
			String lv_col;
			String lv_filter;
			String lv_log = lv_tipo.toUpperCase();
			
			lv_col = lv_tipo + ".*";
			
			lv_filter = lv_tipo + ".uid";
			
			//System.out.println("COLUNA: "+lv_col+" FLTRO: "+lv_filter);
			
			//lv_data.printSchema();
			
			Dataset<Row> lv_json;
			
			lv_json = lv_data.select(lv_col)
					         .filter(col(lv_filter).isNotNull())
					         .withColumnRenamed("id.orig_h", "id_orig_h")
					         .withColumnRenamed("id.orig_p", "id_orig_p")
					         .withColumnRenamed("id.resp_h", "id_resp_h")
					         .withColumnRenamed("id.resp_p", "id_resp_p")					       
					         .withColumn("tipo", functions.lit(lv_log))					
					         .withColumn("ts_code", functions.lit(lv_stamp))
					         .withColumn("row_id", functions.monotonically_increasing_id());
			
			if(lv_tipo.equals(gc_ssh)) {
				lv_json = lv_json.withColumnRenamed("version", "VERSION1");
			}
			
			if(lv_tipo.equals(gc_dhcp)) {
				lv_json = lv_json.withColumnRenamed("trans_id", "TRANS_ID1");
			}
			
			/*lv_json.printSchema();
			lv_json.show();*/
					
			long lv_num = lv_json.count();

			if (lv_num > 0) {
				
				try {
					
				lv_json.write()
				       .format("org.apache.phoenix.spark")
				       .mode("overwrite")
				       .option("table", gv_table)
				       .option("zkUrl", gv_zkurl).save();

				System.out.println("LOG: " + lv_tipo + " = " + lv_num);

				} catch (Exception e) {
					System.out.println("LOG:\t" + lv_tipo + " Erro ao SALVAR dados no HBase: "+e);
				}
				
			}
			
		} catch (Exception e) {
			//System.out.println("LOG:" + lv_tipo + " Nenhum registro a processar: "+e);
		}
		
	}
	
	public static void m_save_devices(Dataset<Row> lv_data, String lv_tipo, long lv_stamp) {
		
		try {						
			
			String lv_col;
						
			lv_col = lv_tipo + ".*";
			
			Dataset<Row> lv_json;
			
			lv_json = lv_data.select(lv_col)
							 .filter(col("mac").isNotNull())
					         .withColumn("ts_code", functions.lit(lv_stamp));
			
			long lv_num = lv_json.count();
			
			/*lv_json.printSchema();
			lv_json.show();*/
			
			if (lv_num > 0) {
				
				try {
				
				lv_json.write()
				       .format("org.apache.phoenix.spark")
				       .mode("overwrite")
				       .option("table", gv_devices)
				       .option("zkUrl", gv_zkurl).save();

				System.out.println("LOG: " + lv_tipo + " = " + lv_num);

				} catch (Exception e) {
					System.out.println("LOG:" + lv_tipo + " Erro ao SALVAR dados no HBase: "+e);
				}
				
			}
			
		} catch (Exception e) {
			//System.out.println("LOG:" + lv_tipo + " Nenhum registro a processar: "+e);
		}
				
	}
	
}












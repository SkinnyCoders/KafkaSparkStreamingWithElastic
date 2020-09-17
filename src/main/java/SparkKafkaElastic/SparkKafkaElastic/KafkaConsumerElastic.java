package SparkKafkaElastic.SparkKafkaElastic;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

public class KafkaConsumerElastic {
	public static final String topicKafka = "datakodam";
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local[3]").setAppName("Consumer to Elastic")
				.set("es.index.auto.create", "true")
				.set("spark.driver.allowMulipleContexts", "true")
				.set("es.nodes", "localhost:9201");
		
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));
		
		Logger rootLogger = Logger.getRootLogger();
		rootLogger.setLevel(Level.OFF);
		
		Map<String, Object> props = new HashMap<String, Object>();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
		
		Collection<String> kafkaTopic = Arrays.asList(topicKafka.split(","));
		
		JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils
				.createDirectStream(jssc, LocationStrategies.PreferConsistent(), ConsumerStrategies.<String, String>Subscribe(kafkaTopic, props));
		
		JavaDStream<String> recordData = stream.map(new Function<ConsumerRecord<String,String>, String>() {

			@Override
			public String call(ConsumerRecord<String, String> datas) throws Exception {
				// TODO Auto-generated method stub
				return datas.value();
			}
		});
		
		try {
			recordData.foreachRDD(e -> {
				System.out.println(e);
				JavaEsSpark.saveJsonToEs(e, "daerah/_doc");
			});
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}

}

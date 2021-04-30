/**
 * 
 */
package sample;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.json.JSONObject;




/**
 * @author prachisethi
 *
 */
public class KafkaUtility {
	String resp="";
	/** The Constant Kafka host. */
	private static final String KAFKA_HOST = System.getenv("KAFKA_HOST") != null ? System.getenv("KAFKA_HOST")
			: "localhost:9092";

	/** The producer config. */
	private Map<String, Object> producerConfig = new HashMap<String, Object>();

	/** The consumer config. */
	private Map<String, Object> consumerConfig = new HashMap<String, Object>();
	/**
	 * Raise event.
	 *
	 * @param topicName the topic name
	 * @param ob      the data
	 * @return the future
	 */
	public void raiseEvent(String topicName, JSONObject ob) {
		long startTime = System.currentTimeMillis();
	    
	    Properties producerConfig = new Properties();
	    producerConfig.put("bootstrap.servers", KAFKA_HOST);
	    producerConfig.put("key.serializer",
	        "org.apache.kafka.common.serialization.StringSerializer");
	    producerConfig.put("value.serializer",
	        "org.apache.kafka.common.serialization.StringSerializer");
	    producerConfig.put("acks", "1");
	    
	    KafkaProducer<String, String> producer =
	        new KafkaProducer<String, String>(producerConfig);
	    producer.send(new ProducerRecord<>(topicName, ob.toString()),
	        new DemoCallBack(startTime, ob.toString()));

	}

	/**
	 * On event.
	 *
	 * @param topicName     the topic name
	 * @param consumerGroup the consumer group
	 * @param handler       the handler
	 * @throws InterruptedException 
	 */
	public String onEvent(String kafkaTopic, String consumerGroup) throws InterruptedException {
		long startTime = System.currentTimeMillis();
	    System.out.println("startTime: " + startTime + "\nconsumerGroup: " + consumerGroup);
	    Properties consumerConfig = new Properties();
	    consumerConfig.put("bootstrap.servers", KAFKA_HOST);
	    consumerConfig.put("key.deserializer",
	        "org.apache.kafka.common.serialization.StringDeserializer");
	    consumerConfig.put("value.deserializer",
	        "org.apache.kafka.common.serialization.StringDeserializer");
	    consumerConfig.put("group.id", consumerGroup);
	       consumerConfig.put("session.timeout.ms", 9000);
	    consumerConfig.put("enable.auto.commit", "false");
	    consumerConfig.put("auto.commit.interval.ms", "1000");

	    KafkaConsumer<Long, String> consumer = new KafkaConsumer<Long, String>(consumerConfig);

	    Collection<String> kafkaTopicCollection = new HashSet<String>();
	    kafkaTopicCollection.add(kafkaTopic);

	    consumer.subscribe(kafkaTopicCollection);

	    long elapsed = (System.currentTimeMillis() - startTime) / 1000;
	    System.out.println("elapsedTime:" + elapsed);

	    // if (elapsed>5000)
	    // throw new RuntimeException("tiomeout");

	    System.out.println("Received message");
	    System.out.println(kafkaTopic);
	    


	    while (true) {
	      final int recordsCount = 100;
	      int noRecordsCount = 0;
	      final ConsumerRecords<Long, String> consumerRecords = consumer.poll(1000);
	      System.out.println("INSIDE_LOOP \n" + consumerRecords.toString());
	      if (consumerRecords.count() == 0) {
	        noRecordsCount++;
	        if (noRecordsCount > recordsCount)
	          break;
	        else
	          continue;
	      }

	      consumerRecords.forEach(record -> {
	        resp = record.value();
	      });

	      System.out.println("EXIT LOOP");
	      consumer.commitAsync();
	      break;
	    }
	    consumer.close();
	    System.out.println("DONE");

	    return resp;
	    // consumer.close();
	}

	class DemoCallBack implements Callback {

		private final long startTime;
		private final String message;

		public DemoCallBack(long startTime, String message) {
			this.startTime = startTime;
			this.message = message;
		}

		/**
		 * onCompletion method will be called when the record sent to the Kafka Server
		 * has been acknowledged.
		 *
		 * @param metadata  The metadata contains the partition and offset of the
		 *                  record. Null if an error occurred.
		 * @param exception The exception thrown during processing of this record. Null
		 *                  if no error occurred.
		 */
		public void onCompletion(RecordMetadata metadata, Exception exception) {
			long elapsedTime = System.currentTimeMillis() - startTime;
			if (metadata != null) {
				System.out.println("Kafka message( " + message + ") sent to partition(" + metadata.partition() + "), "
						+ "offset(" + metadata.offset() + ") in " + elapsedTime + " ms");
			} else {
				exception.printStackTrace();
			}
		}
	}
}
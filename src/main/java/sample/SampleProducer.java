
package sample;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import java.util.concurrent.ExecutionException;


/**
 * @author prachisethi
 *
 */


public class SampleProducer extends Thread {
	private final KafkaProducer<Integer, String> producer;
    private final String topic;
    private final Boolean isAsync;
 
    public static final String KAFKA_SERVER_URL = "localhost";
    public static final int KAFKA_SERVER_PORT = 9092;
    public static final String CLIENT_ID = "SampleProducer";
 
    public SampleProducer(String topic, Boolean isAsync) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", KAFKA_SERVER_URL + ":" + KAFKA_SERVER_PORT);
        properties.put("client.id", CLIENT_ID);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(properties);
        this.topic = topic;
        this.isAsync = isAsync;
    }
 
    public void run(String Message) throws Exception {
    	
            long startTime = System.currentTimeMillis();
            if (isAsync) { // Send asynchronously
            	producer.send(new ProducerRecord<>(topic,Message), new DemoCallBack(startTime,Message));
            } else { // Send synchronously
                try {
                	producer.send(new ProducerRecord<>(topic,Message)).get();
                    System.out.println("Sent message: "+Message);
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                    // handle the exception
                }
        }
    }
}
 
class DemoCallBack implements Callback {
 
    private final long startTime;
    private final String message;
 
    public DemoCallBack(long startTime, String message) {
        this.startTime = startTime;
        this.message = message;
    }
 
    /**
     * onCompletion method will be called when the record sent to the Kafka Server has been acknowledged.
     *
     * @param metadata  The metadata contains the partition and offset of the record. Null if an error occurred.
     * @param exception The exception thrown during processing of this record. Null if no error occurred.
     */
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        long elapsedTime = System.currentTimeMillis() - startTime;
        if (metadata != null) {
            System.out.println(
                    "message( " + message + ") sent to partition(" + metadata.partition() +
                    "), " +
                    "offset(" + metadata.offset() + ") in " + elapsedTime + " ms");
        } else {
            exception.printStackTrace();
        }
    }
}
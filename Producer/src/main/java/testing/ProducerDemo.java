package testing;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {
	private static int numberOfRecrod = 100;

    public static void main(String[] args) {
    	
    	System.out.println("Starting");
    	
        String bootstrapServers = "SSL://b-2.awskafkatutorialcluste.k4skjh.c1.kafka.us-east-1.amazonaws.com:9094,SSL://b-3.awskafkatutorialcluste.k4skjh.c1.kafka.us-east-1.amazonaws.com:9094,SSL://b-1.awskafkatutorialcluste.k4skjh.c1.kafka.us-east-1.amazonaws.com:9094";

        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty("ssl.truststore.location", "/tmp/kafka.client.truststore.jks");
        properties.setProperty("security.protocol", "SSL");

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        System.out.println("Producer Creation : " + producer);
        
        for(int i=1;i<=numberOfRecrod;i++){
			// create a producer record
	        ProducerRecord<String, String> record = new ProducerRecord<String, String>("AWSKafkaTutorialTopic", "Posting message frm laptop -- "+i);
	        producer.send(record);
		}
    
        // flush data
        producer.flush();
        // flush and close producer
        producer.close();
        System.out.println("Completed");

    }
}

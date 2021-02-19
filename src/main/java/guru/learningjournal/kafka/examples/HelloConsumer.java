package guru.learningjournal.kafka.examples;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class HelloConsumer {
    private static final Logger logger = LogManager.getLogger(HelloConsumer.class);
    public static void main(String[] args) {
        if(args.length == 0) {
            System.out.println("Enter topic name");
            return;
        }
        //Kafka consumer configuration settings
        String topicName = args[0];
        Properties props = new Properties();
        logger.info("Starting HelloConsumer...");
        logger.debug("topicName=" + topicName);
        logger.trace("Creating Kafka Consumer...");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "Hello Consumer");
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        KafkaConsumer<Integer, String> consumer = new KafkaConsumer<>(props);
        //Kafka Consumer subscribes list of topics here.
        consumer.subscribe(Collections.singletonList(topicName));

        //print the topic name
        logger.trace("Subscribed to topic " + topicName);

        logger.trace("Start reading messages...");

        while (true) {
            ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<Integer, String> record : records)

                // print the offset,key and value for the consumer records.
                logger.trace(String.format("offset = %d, key = %s, value = %s\n",
                        record.offset(), record.key(), record.value()));
        }
    }
}
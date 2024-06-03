import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Arrays;
import java.util.Properties;

public class KafkaConsumerApp {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer myConsumer = new KafkaConsumer<>(props);
        myConsumer.subscribe(Arrays.asList("my-topic"));

        try {
            while (true) {
                ConsumerRecords<String, String> records = myConsumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    // Process each record
                    System.out.println
                            (String.format("Topic: %s, Partition: %d, Offset: %d, Key: %s, Value: %s",
                                    record.topic(), record.partition(), record.offset(), record.key(), record.value()));
                }
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        } finally {
            myConsumer.close();
            System.out.println(" Kakfa Consumer is over ****");
        }
    }
}

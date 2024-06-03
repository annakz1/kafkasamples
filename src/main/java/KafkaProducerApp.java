import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class KafkaProducerApp {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> myProducer = new KafkaProducer<>(props);
        try {
            for (int i = 0; i < 150; i++) {
                myProducer.send(new ProducerRecord<>("my_topic", Integer.toString(i),
                        "My Message: " + i));
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            myProducer.close();
        }

//        ProducerRecord myRecord = new ProducerRecord("my-topic", "My Message 1- from Java");
//        myProducer.send(myRecord); // Best practice: try... catch
//        myProducer.close();
        System.out.println(" Kakfa Producer is over ****");
    }
}

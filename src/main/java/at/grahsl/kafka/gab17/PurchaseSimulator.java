package at.grahsl.kafka.gab17;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.BufferedReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

public class PurchaseSimulator {

    public static KafkaProducer<String, String> producer = null;

    public static void main(String[] args) {

        System.out.println("Press CTRL-C to stop purchase simulation...");

        if(args.length != 2) {
            System.err.println("error: provide broker url and path to file with purchase records");
            System.exit(-1);
        }

        // add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting Down");
            if (producer != null)
                producer.close();
        }));

        // Configuring producer
        Properties props = new Properties();
        props.put("bootstrap.servers", args[0]);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // Starting producer
        producer = new KafkaProducer<>(props);

        // Simulate purchase orders from demo data file
        try (BufferedReader br = Files.newBufferedReader(
                   Paths.get(args[1]), StandardCharsets.UTF_8)
        ) {
            String purchase;
            while ((purchase = br.readLine()) != null) {
                ProducerRecord<String, String> record = new ProducerRecord<>(Constants.PURCHASE_TOPIC, purchase);
                System.out.println("producer sends -> "+purchase);
                producer.send(record, (RecordMetadata r, Exception e) -> {
                    if (e != null) {
                        System.out.println("Error producing to topic " + r.topic());
                        e.printStackTrace();
                    }
                });
                Thread.sleep(Constants.DELAY*2);
            }
        } catch (Exception exc) {
            exc.printStackTrace();
        }

    }

}

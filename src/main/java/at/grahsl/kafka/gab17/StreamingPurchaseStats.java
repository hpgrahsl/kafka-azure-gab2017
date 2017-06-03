package at.grahsl.kafka.gab17;

import at.grahsl.kafka.gab17.model.Purchase;
import at.grahsl.kafka.gab17.model.PurchaseStats;
import at.grahsl.kafka.gab17.serde.JsonDeserializer;
import at.grahsl.kafka.gab17.serde.JsonSerializer;
import at.grahsl.kafka.gab17.serde.WrapperSerde;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;

import java.time.Duration;
import java.util.Properties;

public class StreamingPurchaseStats {

    static public final class PurchaseSerde extends WrapperSerde<Purchase> {
        public PurchaseSerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>(Purchase.class));
        }
    }

    static public final class PurchaseStatsSerde extends WrapperSerde<PurchaseStats> {
        public PurchaseStatsSerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>(PurchaseStats.class));
        }
    }

    public static void main(String[] args) throws Exception {

        if(args.length < 1 ) {
            System.err.println("error: provide broker url");
            System.exit(-1);
        }

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "purchasestats");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, args[0]);
        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, PurchaseSerde.class.getName());

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(CommonClientConfigs.METADATA_MAX_AGE_CONFIG, 500);

        KStreamBuilder builder = new KStreamBuilder();

        KStream<String, Purchase> topic = builder.stream(Constants.PURCHASE_TOPIC);

        KStream<Windowed<String>, PurchaseStats> pStats =
                topic.map((k,v) -> new KeyValue<>(v.paymentType,v))
                        .through("ps-map-repartition")
                        .aggregateByKey(
                                PurchaseStats::new,
                                (k,v,ps) -> ps.accumulate(v),
                                TimeWindows.of("agg-stats-store",Duration.ofSeconds(10).toMillis()),
                                new Serdes.StringSerde(),
                                new PurchaseStatsSerde()
                        ).toStream().mapValues(ps -> ps.calcAvgTotal());

        pStats.print();

        KafkaStreams streams = new KafkaStreams(builder, props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        //usually runs "forever" in production... we stop after some minutes
        Thread.sleep(Duration.ofMinutes(15).toMillis());
        streams.close();

    }

}


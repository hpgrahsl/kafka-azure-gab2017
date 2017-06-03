package at.grahsl.kafka.gab17;

import at.grahsl.kafka.gab17.model.HashtagCount;
import at.grahsl.kafka.gab17.model.TopHashTags;
import at.grahsl.kafka.gab17.model.Tweet;
import at.grahsl.kafka.gab17.serde.JsonDeserializer;
import at.grahsl.kafka.gab17.serde.JsonSerializer;
import at.grahsl.kafka.gab17.serde.TopNSerdeHT;
import at.grahsl.kafka.gab17.serde.WrapperSerde;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import java.time.Duration;
import java.util.Properties;

public class StreamingTweetsTrackerHTC {

    static public final class TweetSerde extends WrapperSerde<Tweet> {
        public TweetSerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>(Tweet.class));
        }
    }

    static public final class HashtagCountSerde extends WrapperSerde<HashtagCount> {
        public HashtagCountSerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>(HashtagCount.class));
        }
    }

    public static void main(String[] args) throws Exception {

        if(args.length < 1 ) {
            System.err.println("error: provide broker url");
            System.exit(-1);
        }

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "tweetstracker-htc");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, args[0]);
        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, TweetSerde.class.getName());

        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
        // Note: To re-run the demo, you need to use the offset reset tool:
        // https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Streams+Application+Reset+Tool
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // work-around for an issue around timing of creating internal topics
        // this was resolved in 0.10.2.0 and above
        // don't use in large production apps - this increases network load
        props.put(CommonClientConfigs.METADATA_MAX_AGE_CONFIG, 500);

        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();

        KStreamBuilder builder = new KStreamBuilder();

        KStream<String, Tweet> tweets = builder.stream(Constants.TWEETS_TOPIC);

        final KStream<String,Long> hashtagCounts = tweets
                .flatMapValues(t -> t.hashtags)
                .map((k,hashtag) -> new KeyValue<>(hashtag.toLowerCase(),""))
                .through(stringSerde,stringSerde,"tt-htc-repartition")
                .countByKey("hashtag-counts")
                .toStream();

        hashtagCounts.to(stringSerde,longSerde,"hashtag-counts");

        HashtagCountSerde htcSerde = new HashtagCountSerde();
        TopNSerdeHT topNSerde = new TopNSerdeHT();

        final KStream<String,TopHashTags> hashtagAgg = hashtagCounts
                .map((ht, cnt) -> new KeyValue<>("",new HashtagCount(ht,cnt)))
                .through(stringSerde,htcSerde,"tt-topn-repartition")
                .aggregateByKey(
                        ()-> new TopHashTags(TopHashTags.DEFAULT_LIMIT),
                        (aggKey, value, aggregate) -> {
                            aggregate.add(value);
                            return aggregate;
                        },
                        stringSerde,
                        topNSerde,
                        "top-n-hashtags"
                ).toStream();

        hashtagAgg.print();

        KafkaStreams streams = new KafkaStreams(builder, props);
        streams.start();

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        // usually the stream application would be running forever,
        // in this example we just let it run for some time and stop since the input data is finite.
        Thread.sleep(Duration.ofMinutes(15).toMillis());
        streams.close();

    }

}

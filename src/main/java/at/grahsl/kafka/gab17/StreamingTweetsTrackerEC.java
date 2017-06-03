package at.grahsl.kafka.gab17;

import at.grahsl.kafka.gab17.model.EmojiCount;
import at.grahsl.kafka.gab17.model.TopEmojis;
import at.grahsl.kafka.gab17.model.Tweet;
import at.grahsl.kafka.gab17.serde.JsonDeserializer;
import at.grahsl.kafka.gab17.serde.JsonSerializer;
import at.grahsl.kafka.gab17.serde.TopNSerdeEC;
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

public class StreamingTweetsTrackerEC {

    static public final class TweetSerde extends WrapperSerde<Tweet> {
        public TweetSerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>(Tweet.class));
        }
    }

    static public final class EmojiCountSerde extends WrapperSerde<EmojiCount> {
        public EmojiCountSerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>(EmojiCount.class));
        }
    }

    public static void main(String[] args) throws Exception {

        if(args.length < 1 ) {
            System.err.println("error: provide broker url");
            System.exit(-1);
        }

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "tweetstracker-ec");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, args[0]);
        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, TweetSerde.class.getName());

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(CommonClientConfigs.METADATA_MAX_AGE_CONFIG, 500);

        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();

        KStreamBuilder builder = new KStreamBuilder();

        KStream<String, Tweet> tweets = builder.stream(Constants.TWEETS_TOPIC);

        final KStream<String,Long> emojiCounts = tweets
                .flatMapValues(t -> t.emojis)
                .map((k,emoji) -> new KeyValue<>(emoji,""))
                .through(stringSerde,stringSerde,"tt-ec-repartition")
                .countByKey("emoji-counts")
                .toStream();

        emojiCounts.to(stringSerde,longSerde,"emoji-counts");

        EmojiCountSerde ecSerde = new EmojiCountSerde();
        TopNSerdeEC topNSerde = new TopNSerdeEC();

        final KStream<String,TopEmojis> emojiAgg = emojiCounts
                .map((e, cnt) -> new KeyValue<>("",new EmojiCount(e,cnt)))
                .through(stringSerde,ecSerde,"tt-topn-ec-repartition")
                .aggregateByKey(
                        ()-> new TopEmojis(TopEmojis.DEFAULT_LIMIT),
                        (aggKey, value, aggregate) -> {
                            aggregate.add(value);
                            return aggregate;
                        },
                        stringSerde,
                        topNSerde,
                        "top-n-emojis"
                ).toStream();

        emojiAgg.print();

        KafkaStreams streams = new KafkaStreams(builder, props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        //usually runs "forever" in production... we stop after some minutes
        Thread.sleep(Duration.ofMinutes(15).toMillis());
        streams.close();

    }

}

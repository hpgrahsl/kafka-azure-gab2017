package at.grahsl.kafka.gab17;

import at.grahsl.kafka.gab17.model.Tweet;
import at.grahsl.kafka.gab17.serde.JsonSerializer;
import at.grahsl.kafka.gab17.twitter.TweetsListener;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;

import java.util.Arrays;
import java.util.Properties;

public class TweetsProducer {

    public static void main(String[] args) {

        if(args.length < 1 ) {
            System.err.println("error: provide broker url and optional filter keywords");
            System.exit(-1);
        }

        JsonSerializer<Tweet> tweetSerializer = new JsonSerializer<>();

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, args[0]);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, tweetSerializer.getClass().getName());

        Producer<String, Tweet> producer = new KafkaProducer<>(props);

        TwitterStream twitterStream = new TwitterStreamFactory().getInstance();
        twitterStream.addListener(new TweetsListener(producer));

        if(args.length > 1) {
            System.out.println("using args as keywords for twitter stream filter");
            String[] keywords = Arrays.copyOfRange(args,1,args.length);
            System.out.println(Arrays.toString(keywords));
            twitterStream.filter(keywords);
        } else {
            System.out.println("no args as keywords -> sampling tweets");
            twitterStream.sample();
        }

    }

}

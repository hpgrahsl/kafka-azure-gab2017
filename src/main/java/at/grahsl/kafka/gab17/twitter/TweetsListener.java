package at.grahsl.kafka.gab17.twitter;

import at.grahsl.kafka.gab17.Constants;
import at.grahsl.kafka.gab17.emoji.EmojiUtils;
import at.grahsl.kafka.gab17.model.Tweet;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;

import java.util.Arrays;
import java.util.stream.Collectors;

public class TweetsListener implements StatusListener {

    private final Producer<String, Tweet> producer;

    public TweetsListener(Producer<String, Tweet> producer) {
        this.producer = producer;
    }

    @Override
    public void onStatus(Status status) {
        try {

            Tweet t = new Tweet();
            t.id = status.getId();
            t.timestamp = status.getCreatedAt().getTime();
            t.text = status.getText();
            t.lang = status.getLang();
            t.user = status.getUser().getScreenName();
            t.hashtags = Arrays.stream(status.getHashtagEntities())
                    .map(hte -> hte.getText())
                    .collect(Collectors.toList());
            t.emojis = EmojiUtils.extractEmojisAsString(status.getText());

            ProducerRecord<String, Tweet> record =
                    new ProducerRecord<>(Constants.TWEETS_TOPIC, t);

             producer.send(record);

            //demo output only
            System.out.println(t);

        } catch (Exception exc) {
            exc.printStackTrace();
        }
    }

    @Override
    public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
        System.out.println("Got a status deletion notice id:" + statusDeletionNotice.getStatusId());
    }

    @Override
    public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
        System.err.println("Got track limitation notice:" + numberOfLimitedStatuses);
    }

    @Override
    public void onScrubGeo(long userId, long upToStatusId) {
        System.out.println("Got scrub_geo event userId:" + userId + " upToStatusId:" + upToStatusId);
    }

    @Override
    public void onStallWarning(StallWarning warning) {
        System.err.println("Got stall warning:" + warning);
    }

    @Override
    public void onException(Exception exc) {
        exc.printStackTrace();
    }

}

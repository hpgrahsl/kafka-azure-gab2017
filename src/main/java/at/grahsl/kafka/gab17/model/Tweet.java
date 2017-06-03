package at.grahsl.kafka.gab17.model;

import java.util.List;

public class Tweet {

    public long id;
    public long timestamp;
    public String text;
    public String lang;
    public String user;
    public List<String> hashtags;
    public List<String> emojis;

    @Override
    public String toString() {
        return "Tweet{" +
                "id=" + id +
                ", timestamp=" + timestamp +
                ", text='" + text + '\'' +
                ", lang='" + lang + '\'' +
                ", user='" + user + '\'' +
                ", hashtags=" + hashtags +
                ", emojis=" + emojis +
                '}';
    }

}

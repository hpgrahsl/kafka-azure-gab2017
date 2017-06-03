package at.grahsl.kafka.gab17.model;

public class HashtagCount {

    private final String hashtag;
    private final Long count;

    public HashtagCount(String hashtag, Long count) {
        this.hashtag = hashtag;
        this.count = count;
    }

    public String getHashtag() {
        return hashtag;
    }

    public Long getCount() {
        return count;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        HashtagCount that = (HashtagCount) o;

        if (!hashtag.equals(that.hashtag)) return false;
        return count.equals(that.count);
    }

    @Override
    public int hashCode() {
        int result = hashtag.hashCode();
        result = 31 * result + count.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return hashtag +" -> "+count;
    }

}

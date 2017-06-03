package at.grahsl.kafka.gab17.model;

import java.util.*;

public class TopHashTags implements Iterable<HashtagCount> {

    public static final int DEFAULT_LIMIT = 10;

    private final Map<String,HashtagCount> hashtags = new HashMap<>(DEFAULT_LIMIT);

    private final TreeSet<HashtagCount> topN = new TreeSet<>(
            Comparator.comparingLong(HashtagCount::getCount).reversed()
                    .thenComparing(Comparator.comparing(HashtagCount::getHashtag)));

    private final int limit;

    public TopHashTags(int limit) {
        this.limit = limit;
    }

    public void add(final HashtagCount htc) {

        if(hashtags.containsKey(htc.getHashtag())) {
            topN.remove(hashtags.remove(htc.getHashtag()));
        }
        topN.add(htc);
        hashtags.put(htc.getHashtag(),htc);
        if(topN.size() > limit) {
            final HashtagCount lowest = topN.last();
            topN.remove(hashtags.remove(lowest.getHashtag()));
        }
    }

    public void remove(final HashtagCount htc) {
        hashtags.remove(htc);
        topN.remove(htc);
    }

    @Override
    public Iterator<HashtagCount> iterator() {
        return topN.iterator();
    }

    @Override
    public String toString() {
        return "TopHashTags{" +
                "topN=" + topN +
                '}';
    }
}

package at.grahsl.kafka.gab17.model;

public class EmojiCount {

    private final String emoji;
    private final Long count;

    public EmojiCount(String emoji, Long count) {
        this.emoji = emoji;
        this.count = count;
    }

    public String getEmoji() {
        return emoji;
    }

    public Long getCount() {
        return count;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        EmojiCount that = (EmojiCount) o;

        if (!emoji.equals(that.emoji)) return false;
        return count.equals(that.count);
    }

    @Override
    public int hashCode() {
        int result = emoji.hashCode();
        result = 31 * result + count.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return emoji +" -> "+count;
    }
}

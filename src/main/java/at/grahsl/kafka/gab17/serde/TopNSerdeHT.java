package at.grahsl.kafka.gab17.serde;

import at.grahsl.kafka.gab17.model.HashtagCount;
import at.grahsl.kafka.gab17.model.TopHashTags;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.io.*;
import java.util.Map;

public class TopNSerdeHT implements Serde<TopHashTags> {

    @Override
    public Serializer<TopHashTags> serializer() {
        return new Serializer<TopHashTags>() {

            @Override
            public void configure(final Map<String, ?> map, final boolean b) {
            }

            @Override
            public byte[] serialize(final String s, final TopHashTags tophashtags) {
                final ByteArrayOutputStream out = new ByteArrayOutputStream();
                final DataOutputStream
                        dataOutputStream =
                        new DataOutputStream(out);
                try {
                    for (HashtagCount htc : tophashtags) {
                        dataOutputStream.writeUTF(htc.getHashtag());
                        dataOutputStream.writeLong(htc.getCount());
                    }
                    dataOutputStream.flush();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                return out.toByteArray();
            }

            @Override
            public void close() {

            }
        };
    }

    @Override
    public Deserializer<TopHashTags> deserializer() {
        return new Deserializer<TopHashTags>() {

            @Override
            public void configure(final Map<String, ?> map, final boolean b) {

            }

            @Override
            public TopHashTags deserialize(final String s, final byte[] bytes) {
                if (bytes == null || bytes.length == 0) {
                    return null;
                }
                final TopHashTags result = new TopHashTags(TopHashTags.DEFAULT_LIMIT);

                final DataInputStream
                        dataInputStream =
                        new DataInputStream(new ByteArrayInputStream(bytes));

                try {
                    while(dataInputStream.available() > 0) {
                        result.add(new HashtagCount(dataInputStream.readUTF(),
                                dataInputStream.readLong()));
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                return result;
            }

            @Override
            public void close() {

            }
        };
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public void close() {

    }

}

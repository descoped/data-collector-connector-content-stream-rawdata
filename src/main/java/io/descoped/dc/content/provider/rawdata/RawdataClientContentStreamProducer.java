package io.descoped.dc.content.provider.rawdata;

import com.fasterxml.jackson.databind.node.ArrayNode;
import io.descoped.dc.api.content.ClosedContentStreamException;
import io.descoped.dc.api.content.ContentStreamBuffer;
import io.descoped.dc.api.content.ContentStreamProducer;
import io.descoped.dc.api.util.JsonParser;
import io.descoped.rawdata.api.RawdataMessage;
import io.descoped.rawdata.api.RawdataProducer;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;

public class RawdataClientContentStreamProducer implements ContentStreamProducer {

    private final RawdataProducer producer;
    private final Consumer<String> closeAndRemoveProducer;
    private final Function<byte[], byte[]> tryEncryptContent;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public RawdataClientContentStreamProducer(RawdataProducer producer, Consumer<String> closeAndRemoveProducer, Function<byte[], byte[]> tryEncryptContent) {
        this.producer = producer;
        this.closeAndRemoveProducer = closeAndRemoveProducer;
        this.tryEncryptContent = tryEncryptContent;
    }

    @Override
    public ContentStreamBuffer.Builder builder() {
        if (isClosed()) {
            throw new ClosedContentStreamException();
        }
        return new RawdataClientContentStreamBuffer.Builder();
    }

    @Override
    public ContentStreamProducer copy(ContentStreamBuffer buffer) {
        if (isClosed()) {
            throw new ClosedContentStreamException();
        }

        RawdataMessage.Builder messageBuilder = RawdataMessage.builder();

        messageBuilder.ulid(buffer.ulid());
        messageBuilder.position(buffer.position());

        for (String key : buffer.keys()) {
            messageBuilder.put(key, buffer.get(key));
        }

        producer.publish(messageBuilder.build());

        return this;
    }

    @Override
    public ContentStreamProducer produce(ContentStreamBuffer.Builder bufferBuilder) {
        if (isClosed()) {
            throw new ClosedContentStreamException();
        }
        JsonParser jsonParser = JsonParser.createJsonParser();
        ArrayNode arrayNode = jsonParser.createArrayNode();
        bufferBuilder.manifest().forEach(metadataContent -> arrayNode.add(metadataContent.getElementNode()));
        byte[] manifestJson = jsonParser.toJSON(arrayNode).getBytes();

        if (tryEncryptContent != null) {
            manifestJson = tryEncryptContent.apply(manifestJson);
        }

        bufferBuilder.buffer("manifest.json", manifestJson, null); // commit generated manifestList as json when Manifest(Entry) is null.

        ContentStreamBuffer contentBuffer = bufferBuilder.build();

        RawdataMessage.Builder messageBuilder = RawdataMessage.builder();

        if (contentBuffer.ulid() != null) {
            messageBuilder.ulid(contentBuffer.ulid());
        }
        messageBuilder.position(contentBuffer.position());

        // content encryption has already been done in RawdataClientContentStore
        for (Map.Entry<String, byte[]> entry : contentBuffer.data().entrySet()) {
            messageBuilder.put(entry.getKey(), entry.getValue());
        }

        producer.publish(messageBuilder.build());

        return this;
    }

    @Deprecated
    @Override
    public void publish(String... position) {
        if (isClosed()) {
            throw new ClosedContentStreamException();
        }
//        producer.publish(position);
    }

    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public void close() throws Exception {
        if (closed.compareAndSet(false, true)) {
            closeAndRemoveProducer.accept(producer.topic());
            producer.close();
        }
    }

}

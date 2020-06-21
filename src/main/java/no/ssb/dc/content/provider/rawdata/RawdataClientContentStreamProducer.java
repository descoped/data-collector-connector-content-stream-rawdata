package no.ssb.dc.content.provider.rawdata;

import com.fasterxml.jackson.databind.node.ArrayNode;
import no.ssb.dc.api.content.ClosedContentStreamException;
import no.ssb.dc.api.content.ContentStreamBuffer;
import no.ssb.dc.api.content.ContentStreamProducer;
import no.ssb.dc.api.util.JsonParser;
import no.ssb.rawdata.api.RawdataMessage;
import no.ssb.rawdata.api.RawdataProducer;

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

        bufferBuilder.buffer("manifest.json", manifestJson, null);

        ContentStreamBuffer contentBuffer = bufferBuilder.build();

        RawdataMessage.Builder messageBuilder = producer.builder();

        messageBuilder.position(contentBuffer.position());

        for (Map.Entry<String, byte[]> entry : contentBuffer.data().entrySet()) {
            messageBuilder.put(entry.getKey(), entry.getValue());
        }

        producer.buffer(messageBuilder);

        return this;
    }

    @Override
    public void publish(String... position) {
        if (isClosed()) {
            throw new ClosedContentStreamException();
        }
        producer.publish(position);
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

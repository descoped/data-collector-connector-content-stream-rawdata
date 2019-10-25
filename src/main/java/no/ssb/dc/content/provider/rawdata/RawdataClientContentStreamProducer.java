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

public class RawdataClientContentStreamProducer implements ContentStreamProducer {

    private final RawdataProducer producer;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public RawdataClientContentStreamProducer(RawdataProducer producer) {
        this.producer = producer;
    }

    @Override
    public ContentStreamBuffer.Builder builder() {
        if (isClosed()) {
            throw new ClosedContentStreamException();
        }
        return new RawdataClientContentStreamBuffer.Builder();
    }

    @Override
    public void produce(ContentStreamBuffer.Builder bufferBuilder) {
        if (isClosed()) {
            throw new ClosedContentStreamException();
        }
        JsonParser jsonParser = JsonParser.createJsonParser();
        ArrayNode arrayNode = jsonParser.createArrayNode();
        bufferBuilder.manifest().forEach(metadataContent -> arrayNode.add(metadataContent.getElementNode()));
        byte[] manifestJson = jsonParser.toJSON(arrayNode).getBytes();
        bufferBuilder.buffer("manifest.json", manifestJson, null);

        ContentStreamBuffer contentBuffer = bufferBuilder.build();

        RawdataMessage.Builder messageBuilder = producer.builder();

        messageBuilder.position(contentBuffer.position());

        for (Map.Entry<String, byte[]> entry : contentBuffer.data().entrySet()) {
            messageBuilder.put(entry.getKey(), entry.getValue());
        }

        producer.buffer(messageBuilder);
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
            producer.close();
        }
    }

}

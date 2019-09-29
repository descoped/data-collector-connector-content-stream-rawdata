package no.ssb.dc.content.provider.rawdata;

import com.fasterxml.jackson.databind.node.ArrayNode;
import no.ssb.dc.api.content.ContentStreamBuffer;
import no.ssb.dc.api.content.ContentStreamProducer;
import no.ssb.dc.api.util.JacksonFactory;
import no.ssb.rawdata.api.RawdataMessage;
import no.ssb.rawdata.api.RawdataProducer;

import java.util.Map;

public class RawdataClientContentStreamProducer implements ContentStreamProducer {

    final RawdataProducer producer;

    public RawdataClientContentStreamProducer(RawdataProducer producer) {
        this.producer = producer;
    }

    @Override
    public ContentStreamBuffer.Builder builder() {
        return new RawdataClientContentStreamBuffer.Builder();
    }

    @Override
    public void produce(ContentStreamBuffer.Builder bufferBuilder) {
        ArrayNode arrayNode = JacksonFactory.instance().createArrayNode();
        bufferBuilder.manifest().forEach(metadataContent -> arrayNode.add(metadataContent.getElementNode()));
        byte[] manifestJson = JacksonFactory.instance().toJSON(arrayNode).getBytes();
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
        producer.publish(position);
    }

}

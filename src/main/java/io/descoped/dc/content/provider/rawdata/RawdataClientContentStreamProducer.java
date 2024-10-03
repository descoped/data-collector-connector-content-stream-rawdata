package io.descoped.dc.content.provider.rawdata;

import com.fasterxml.jackson.databind.node.ArrayNode;
import io.descoped.dc.api.content.ClosedContentStreamException;
import io.descoped.dc.api.content.ContentStreamBuffer;
import io.descoped.dc.api.content.ContentStreamProducer;
import io.descoped.dc.api.util.JsonParser;
import io.descoped.rawdata.api.RawdataMessage;
import io.descoped.rawdata.api.RawdataProducer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;

public class RawdataClientContentStreamProducer implements ContentStreamProducer {

    private final RawdataProducer producer;
    private final Consumer<String> closeAndRemoveProducer;
    private final Function<byte[], byte[]> tryEncryptContent;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final Map<String, List<RawdataMessage>> bufferMap;

    public RawdataClientContentStreamProducer(RawdataProducer producer, Consumer<String> closeAndRemoveProducer, Function<byte[], byte[]> tryEncryptContent) {
        this.producer = producer;
        this.closeAndRemoveProducer = closeAndRemoveProducer;
        this.tryEncryptContent = tryEncryptContent;
        this.bufferMap = new ConcurrentHashMap<>();
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

        RawdataMessage message = createRawdataMessage(buffer);
        bufferMessage(message);

        return this;
    }

    @Override
    public ContentStreamProducer produce(ContentStreamBuffer.Builder bufferBuilder) {
        if (isClosed()) {
            throw new ClosedContentStreamException();
        }

        ContentStreamBuffer contentBuffer = prepareContentBuffer(bufferBuilder);
        RawdataMessage message = createRawdataMessage(contentBuffer);
        bufferMessage(message);

        return this;
    }

    @Override
    public void publish(String... positions) throws ClosedContentStreamException {
        if (isClosed()) {
            throw new ClosedContentStreamException();
        }

        List<RawdataMessage> messages = new ArrayList<>();
        for (String position : positions) {
            List<RawdataMessage> bufferedMessages = bufferMap.remove(position);
            if (bufferedMessages != null) {
                messages.addAll(bufferedMessages);
            }
        }

        if (!messages.isEmpty()) {
            producer.publish(messages.toArray(new RawdataMessage[0]));
        }
    }

    private ContentStreamBuffer prepareContentBuffer(ContentStreamBuffer.Builder bufferBuilder) {
        JsonParser jsonParser = JsonParser.createJsonParser();
        ArrayNode arrayNode = jsonParser.createArrayNode();
        bufferBuilder.manifest().forEach(metadataContent -> arrayNode.add(metadataContent.getElementNode()));
        byte[] manifestJson = jsonParser.toJSON(arrayNode).getBytes();

        if (tryEncryptContent != null) {
            manifestJson = tryEncryptContent.apply(manifestJson);
        }

        bufferBuilder.buffer("manifest.json", manifestJson, null);

        return bufferBuilder.build();
    }

    private RawdataMessage createRawdataMessage(ContentStreamBuffer buffer) {
        RawdataMessage.Builder messageBuilder = RawdataMessage.builder();

        if (buffer.ulid() != null) {
            messageBuilder.ulid(buffer.ulid());
        }
        messageBuilder.position(buffer.position());

        for (Map.Entry<String, byte[]> entry : buffer.data().entrySet()) {
            messageBuilder.put(entry.getKey(), entry.getValue());
        }

        return messageBuilder.build();
    }

    private void bufferMessage(RawdataMessage message) {
        bufferMap.computeIfAbsent(message.position(), k -> new ArrayList<>()).add(message);
    }

    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public void close() throws Exception {
        if (closed.compareAndSet(false, true)) {
            try {
                // Publish all remaining buffered messages
                publishAllBufferedMessages();
            } finally {
                closeAndRemoveProducer.accept(producer.topic());
                producer.close();
            }
        }
    }

    private void publishAllBufferedMessages() {
        List<RawdataMessage> allMessages = new ArrayList<>();
        for (List<RawdataMessage> messages : bufferMap.values()) {
            allMessages.addAll(messages);
        }

        if (!allMessages.isEmpty()) {
            producer.publish(allMessages.toArray(new RawdataMessage[0]));
        }

        // Clear the buffer after publishing
        bufferMap.clear();
    }
}
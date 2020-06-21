package no.ssb.dc.content.provider.rawdata;

import no.ssb.dc.api.content.ClosedContentStreamException;
import no.ssb.dc.api.content.ContentStream;
import no.ssb.dc.api.content.ContentStreamConsumer;
import no.ssb.dc.api.content.ContentStreamCursor;
import no.ssb.dc.api.content.ContentStreamProducer;
import no.ssb.rawdata.api.RawdataClient;
import no.ssb.rawdata.api.RawdataMessage;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

public class RawdataClientContentStream implements ContentStream {

    private final RawdataClient client;
    private final Function<byte[], byte[]> tryEncryptContent;
    private final Map<String, ContentStreamProducer> producerMap = new ConcurrentHashMap<>();
    private final Map<String, ContentStreamConsumer> consumerMap = new ConcurrentHashMap<>();
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public RawdataClientContentStream(RawdataClient client) {
        this(client, null);
    }

    public RawdataClientContentStream(RawdataClient client, Function<byte[], byte[]> tryEncryptContent) {
        this.client = client;
        this.tryEncryptContent = tryEncryptContent;
    }

    @Override
    public String lastPosition(String topic) {
        if (isClosed()) {
            throw new ClosedContentStreamException();
        }
        RawdataMessage message = client.lastMessage(topic);
        return message != null ? message.position() : null;
    }

    @Override
    public ContentStreamProducer producer(String topic) {
        if (isClosed()) {
            throw new ClosedContentStreamException();
        }
        return producerMap.computeIfAbsent(topic, p -> new RawdataClientContentStreamProducer(client.producer(topic), this::closeAndRemoveProducer, tryEncryptContent));
    }

    @Override
    public ContentStreamConsumer consumer(String topic, ContentStreamCursor cursor) {
        if (isClosed()) {
            throw new ClosedContentStreamException();
        }
        return consumerMap.computeIfAbsent(topic, c -> new RawdataClientContentStreamConsumer(client.consumer(topic), this::closeAndRemoveConsumer));
    }

    @Override
    public void closeAndRemoveProducer(String topic) {
        ContentStreamProducer producer = producerMap.remove(topic);
        if (producer != null) {
            try {
                producer.close();
            } catch (RuntimeException | Error e) {
                throw e;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void closeAndRemoveConsumer(String topic) {
        ContentStreamConsumer consumer = consumerMap.remove(topic);
        if (consumer != null) {
            try {
                consumer.close();
            } catch (RuntimeException | Error e) {
                throw e;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public void close() throws Exception {
        if (closed.compareAndSet(false, true)) {
            for (ContentStreamProducer producer : producerMap.values()) {
                producer.close();
            }
            for (ContentStreamConsumer consumer : consumerMap.values()) {
                consumer.close();
            }
            producerMap.clear();
            consumerMap.clear();
            client.close();
        }
    }
}

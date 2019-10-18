package no.ssb.dc.content.provider.rawdata;

import no.ssb.dc.api.content.ContentStateKey;
import no.ssb.dc.api.content.ContentStore;
import no.ssb.dc.api.content.ContentStreamBuffer;
import no.ssb.dc.api.content.ContentStreamProducer;
import no.ssb.dc.api.content.HttpRequestInfo;
import no.ssb.dc.api.content.MetadataContent;
import no.ssb.rawdata.api.RawdataClient;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class RawdataClientContentStore implements ContentStore {

    private final RawdataClientContentStream contentStream;
    private final Map<ContentStateKey, ContentStreamBuffer.Builder> contentBuffers = new ConcurrentHashMap<>();
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public RawdataClientContentStore(RawdataClient client) {
        contentStream = new RawdataClientContentStream(client);
    }

    @Override
    public String lastPosition(String topic) {
        return contentStream.lastPosition(topic);
    }

    @Override
    public Set<String> contentKeys(String topic, String position) {
        ContentStreamBuffer.Builder builder = contentBuffers.get(new ContentStateKey(topic, position));
        return (builder == null ? new HashSet<>() : builder.keys());
    }

    @Override
    public void addPaginationDocument(String topic, String contentKey, byte[] content, HttpRequestInfo httpRequestInfo) {
        ContentStreamProducer producer = contentStream.producer(topic + "-pages");
        ContentStreamBuffer.Builder bufferBuilder = producer.builder();

        String position = httpRequestInfo.getCorrelationIds().first().toString();
        bufferBuilder.position(position);

        MetadataContent manifest = getMetadataContent(topic + "-pages", position, contentKey, content, MetadataContent.ResourceType.PAGE, httpRequestInfo);

        bufferBuilder.buffer(contentKey, content, manifest);
        producer.produce(bufferBuilder);

        producer.publish(position);
    }

    @Override
    public void bufferPaginationEntryDocument(String topic, String position, String contentKey, byte[] content, HttpRequestInfo httpRequestInfo) {
        ContentStreamProducer producer = contentStream.producer(topic);
        ContentStreamBuffer.Builder bufferBuilder = contentBuffers.computeIfAbsent(new ContentStateKey(topic, position), contentBuilder -> producer.builder());
        MetadataContent manifest = getMetadataContent(topic, position, contentKey, content, MetadataContent.ResourceType.ENTRY, httpRequestInfo);
        bufferBuilder.position(position).buffer(contentKey, content, manifest);
    }

    @Override
    public void bufferDocument(String topic, String position, String contentKey, byte[] content, HttpRequestInfo httpRequestInfo) {
        ContentStreamProducer producer = contentStream.producer(topic);
        ContentStreamBuffer.Builder bufferBuilder = contentBuffers.computeIfAbsent(new ContentStateKey(topic, position), contentBuilder -> producer.builder());
        MetadataContent manifest = getMetadataContent(topic, position, contentKey, content, MetadataContent.ResourceType.DOCUMENT, httpRequestInfo);
        bufferBuilder.position(position).buffer(contentKey, content, manifest);
    }

    @Override
    public void publish(String topic, String... positions) {
        ContentStreamProducer producer = contentStream.producer(topic);
        for (String position : positions) {
            ContentStateKey contentStateKey = new ContentStateKey(topic, position);
            ContentStreamBuffer.Builder bufferBuilder = contentBuffers.computeIfAbsent(contentStateKey, contentBuilder -> producer.builder());

            producer.produce(bufferBuilder);
            contentBuffers.remove(contentStateKey);
        }
        producer.publish(positions);
    }

    MetadataContent getMetadataContent(String topic, String position, String contentKey, byte[] content, MetadataContent.ResourceType resourceType, HttpRequestInfo httpRequestInfo) {
        return new MetadataContent.Builder()
                .resourceType(resourceType)
                .correlationId(httpRequestInfo.getCorrelationIds())
                .url(httpRequestInfo.getUrl())
                .topic(topic)
                .position(position)
                .contentKey(contentKey)
                .contentType(httpRequestInfo.getResponseHeaders().firstValue("content-type").orElseGet(() -> "application/octet-stream"))
                .contentLength(content.length)
                .requestDurationNanoTime(httpRequestInfo.getRequestDurationNanoSeconds())
                .requestHeaders(httpRequestInfo.getRequestHeaders())
                .responseHeaders(httpRequestInfo.getResponseHeaders())
                .build();
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public void close() throws Exception {
        contentStream.close();
        closed.set(true);
    }
}

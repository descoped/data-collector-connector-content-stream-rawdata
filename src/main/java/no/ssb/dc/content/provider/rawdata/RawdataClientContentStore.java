package no.ssb.dc.content.provider.rawdata;

import no.ssb.dc.api.content.ContentStateKey;
import no.ssb.dc.api.content.ContentStore;
import no.ssb.dc.api.content.ContentStreamBuffer;
import no.ssb.dc.api.content.ContentStreamProducer;
import no.ssb.dc.api.content.HttpRequestInfo;
import no.ssb.dc.api.content.MetadataContent;
import no.ssb.dc.api.ulid.ULIDStateHolder;
import no.ssb.rawdata.api.RawdataClient;
import no.ssb.rawdata.api.RawdataClientInitializer;
import no.ssb.service.provider.api.ProviderConfigurator;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class RawdataClientContentStore implements ContentStore {

    private final RawdataClientContentStream contentStream;
    private final ULIDStateHolder ulidStateHolder = new ULIDStateHolder();
    private final Map<ContentStateKey, ContentStreamBuffer.Builder> contentBuffers = new ConcurrentHashMap<>();

    public RawdataClientContentStore(Map<String, String> configuration) {
        RawdataClient client = ProviderConfigurator.configure(configuration, configuration.get("rawdata.client.provider"), RawdataClientInitializer.class);
        contentStream = new RawdataClientContentStream(client);
    }

    @Override
    public String lastPosition(String namespace) {
        return contentStream.lastPosition(namespace);
    }

    @Override
    public Set<String> contentKeys(String namespace, String position) {
        ContentStreamBuffer.Builder builder = contentBuffers.get(new ContentStateKey(namespace, position));
        return (builder == null ? new HashSet<>() : builder.keys());
    }

    @Override
    public void addPaginationDocument(String namespace, String contentKey, byte[] content, HttpRequestInfo httpRequestInfo) {
        ContentStreamProducer producer = contentStream.producer(namespace + "-pages");
        ContentStreamBuffer.Builder bufferBuilder = producer.builder();

        String position = httpRequestInfo.getCorrelationIds().first().toString();
        bufferBuilder.position(position);

        MetadataContent manifest = getMetadataContent(namespace + "-pages", position, contentKey, content, MetadataContent.ResourceType.PAGE, httpRequestInfo);
        bufferBuilder.buffer(contentKey, content, manifest);
        producer.produce(bufferBuilder);
    }

    @Override
    public void bufferPaginationEntryDocument(String namespace, String position, String contentKey, byte[] content, HttpRequestInfo httpRequestInfo) {
        ContentStreamProducer producer = contentStream.producer(namespace);
        ContentStreamBuffer.Builder bufferBuilder = contentBuffers.computeIfAbsent(new ContentStateKey(namespace, position), contentBuilder -> producer.builder());
        MetadataContent manifest = getMetadataContent(namespace, position, contentKey, content, MetadataContent.ResourceType.ENTRY, httpRequestInfo);
        bufferBuilder.position(position).buffer(contentKey, content, manifest);
    }

    @Override
    public void bufferDocument(String namespace, String position, String contentKey, byte[] content, HttpRequestInfo httpRequestInfo) {
        ContentStreamProducer producer = contentStream.producer(namespace);
        ContentStreamBuffer.Builder bufferBuilder = contentBuffers.computeIfAbsent(new ContentStateKey(namespace, position), contentBuilder -> producer.builder());
        MetadataContent manifest = getMetadataContent(namespace, position, contentKey, content, MetadataContent.ResourceType.DOCUMENT, httpRequestInfo);
        bufferBuilder.position(position).buffer(contentKey, content, manifest);
    }

    @Override
    public void publish(String namespace, String... position) {
        ContentStreamProducer producer = contentStream.producer(namespace);
        for (String pos : position) {
            ContentStateKey contentStateKey = new ContentStateKey(namespace, pos);
            ContentStreamBuffer.Builder bufferBuilder = contentBuffers.computeIfAbsent(contentStateKey, contentBuilder -> producer.builder());

            producer.produce(bufferBuilder);
            contentBuffers.remove(contentStateKey);
        }
        producer.publish(position);
    }

    MetadataContent getMetadataContent(String namespace, String position, String contentKey, byte[] content, MetadataContent.ResourceType resourceType, HttpRequestInfo httpRequestInfo) {
        return new MetadataContent.Builder()
                .resourceType(resourceType)
                .correlationId(httpRequestInfo.getCorrelationIds())
                .url(httpRequestInfo.getUrl())
                .namespace(namespace)
                .position(position)
                .contentKey(contentKey)
                .contentType(httpRequestInfo.getResponseHeaders().firstValue("content-type").orElseGet(() -> "application/octet-stream"))
                .contentLength(content.length)
                .requestDurationNanoTime(httpRequestInfo.getRequestDurationNanoSeconds())
                .requestHeaders(httpRequestInfo.getRequestHeaders())
                .responseHeaders(httpRequestInfo.getResponseHeaders())
                .build();
    }

}

package no.ssb.dc.content.provider.rawdata;

import no.ssb.dc.api.content.ContentStateKey;
import no.ssb.dc.api.content.ContentStore;
import no.ssb.dc.api.content.ContentStreamBuffer;
import no.ssb.dc.api.content.ContentStreamProducer;
import no.ssb.dc.api.content.HealthContentStreamMonitor;
import no.ssb.dc.api.content.HttpRequestInfo;
import no.ssb.dc.api.content.MetadataContent;
import no.ssb.rawdata.api.RawdataClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class RawdataClientContentStore implements ContentStore {

    private static final Logger LOG = LoggerFactory.getLogger(RawdataClientContentStore.class);

    private final HealthContentStreamMonitor monitor;
    private final RawdataClientContentStream contentStream;
    private final Map<ContentStateKey, ContentStreamBuffer.Builder> contentBuffers = new ConcurrentHashMap<>();
    private final Map<String, Lock> lockByTopic = new ConcurrentHashMap<>();
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public RawdataClientContentStore(RawdataClient client) {
        contentStream = new RawdataClientContentStream(client);
        monitor = new HealthContentStreamMonitor(this::isClosed);
    }

    @Override
    public void lock(String topic) {
        Lock lock = lockByTopic.computeIfAbsent(topic, t -> new ReentrantLock());
        try {
            lock.lockInterruptibly();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void unlock(String topic) {
        Lock lock = lockByTopic.get(topic);
        lock.unlock();
    }

    @Override
    public String lastPosition(String topic) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Find lastPosition({}){}", topic, locateJavaCall());
        }
        return contentStream.lastPosition(topic);
    }

    private String locateJavaCall() {
        Stream<StackTraceElement> stackTraceList = Arrays.stream(Thread.currentThread().getStackTrace());
        return  "\n\tat " + stackTraceList.skip(2).limit(5).map(StackTraceElement::toString).collect(Collectors.joining("\n\t   "));
    }

    @Override
    public Set<String> contentKeys(String topic, String position) {
        ContentStreamBuffer.Builder builder = contentBuffers.get(new ContentStateKey(topic, position));
        return (builder == null ? new HashSet<>() : builder.keys());
    }

    @Override
    public void addPaginationDocument(String topic, String contentKey, byte[] content, HttpRequestInfo httpRequestInfo) {
        long past = System.currentTimeMillis();
        String paginationDocumentTopic = topic + "-pages";
        ContentStreamProducer producer = contentStream.producer(paginationDocumentTopic);
        ContentStreamBuffer.Builder bufferBuilder = producer.builder();

        String position = httpRequestInfo.getCorrelationIds().first().toString();
        bufferBuilder.position(position);

        MetadataContent manifest = getMetadataContent(paginationDocumentTopic, position, contentKey, content, MetadataContent.ResourceType.PAGE, httpRequestInfo);

        bufferBuilder.buffer(contentKey, content, manifest);
        producer.produce(bufferBuilder);

        producer.publish(position);

        monitor.incrementPaginationDocumentCount();
        monitor.addPaginationDocumentSize(content.length);
        monitor.updateLastPaginationDocumentWriteDuration(System.currentTimeMillis() - past);
    }

    @Override
    public void bufferPaginationEntryDocument(String topic, String position, String contentKey, byte[] content, HttpRequestInfo httpRequestInfo) {
        long past = System.currentTimeMillis();
        ContentStreamProducer producer = contentStream.producer(topic);
        ContentStreamBuffer.Builder bufferBuilder = contentBuffers.computeIfAbsent(new ContentStateKey(topic, position), contentBuilder -> producer.builder());
        MetadataContent manifest = getMetadataContent(topic, position, contentKey, content, MetadataContent.ResourceType.ENTRY, httpRequestInfo);
        bufferBuilder.position(position).buffer(contentKey, content, manifest);

        monitor.incrementEntryBufferCount();
        monitor.addEntryBufferSize(content.length);
        monitor.updateLastEntryBufferWriteDuration(System.currentTimeMillis() - past);
    }

    @Override
    public void bufferDocument(String topic, String position, String contentKey, byte[] content, HttpRequestInfo httpRequestInfo) {
        long past = System.currentTimeMillis();
        ContentStreamProducer producer = contentStream.producer(topic);
        ContentStreamBuffer.Builder bufferBuilder = contentBuffers.computeIfAbsent(new ContentStateKey(topic, position), contentBuilder -> producer.builder());
        MetadataContent manifest = getMetadataContent(topic, position, contentKey, content, MetadataContent.ResourceType.DOCUMENT, httpRequestInfo);
        bufferBuilder.position(position).buffer(contentKey, content, manifest);

        monitor.incrementDocumentBufferCount();
        monitor.addDocumentBufferSize(content.length);
        monitor.updateLastDocumentBufferWriteDuration(System.currentTimeMillis() - past);
    }

    @Override
    public void publish(String topic, String... positions) {
        long past = System.currentTimeMillis();
        ContentStreamProducer producer = contentStream.producer(topic);
        for (String position : positions) {
            ContentStateKey contentStateKey = new ContentStateKey(topic, position);
            ContentStreamBuffer.Builder bufferBuilder = contentBuffers.computeIfAbsent(contentStateKey, contentBuilder -> producer.builder());

            producer.produce(bufferBuilder);
            monitor.addPublishedBufferCount(bufferBuilder.keys().size() - 1); // subtract to not count manifest, because they're not counted in buffering
            monitor.updateLastPublishedBufferCount(bufferBuilder.keys().size() - 1);
            contentBuffers.remove(contentStateKey);
        }
        producer.publish(positions);

        monitor.updateLastSeen();
        monitor.addPublishedPositionCount(positions.length);
        monitor.updateLastPublishedPositionWriteDuration(System.currentTimeMillis() - past);
    }

    @Override
    public HealthContentStreamMonitor monitor() {
        return monitor;
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
    public void closeTopic(String topic) {
        lock(topic);
        try {
            contentStream.closeAndRemoveProducer(topic);
        } finally {
            unlock(topic);
        }
    }

    @Override
    public void close() throws Exception {
        if (closed.compareAndSet(false, true)) {
            contentStream.close();
        }
    }
}

package io.descoped.dc.content.provider.rawdata;

import io.descoped.dc.api.CorrelationIds;
import io.descoped.dc.api.content.ContentStore;
import io.descoped.dc.api.content.ContentStoreInitializer;
import io.descoped.dc.api.content.ContentStream;
import io.descoped.dc.api.content.ContentStreamBuffer;
import io.descoped.dc.api.content.ContentStreamConsumer;
import io.descoped.dc.api.content.ContentStreamProducer;
import io.descoped.dc.api.content.HttpRequestInfo;
import io.descoped.dc.api.context.ExecutionContext;
import io.descoped.dc.api.http.Headers;
import io.descoped.encryption.client.Algorithm;
import io.descoped.encryption.client.EncryptionClient;
import io.descoped.rawdata.api.RawdataClient;
import io.descoped.rawdata.api.RawdataClientInitializer;
import io.descoped.service.provider.api.ProviderConfigurator;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class ContentStreamTest {

    @Test
    public void thatContentStoreInitializer() {
        ContentStore contentStore = ProviderConfigurator.configure(Map.of(
                        "rawdata.client.provider", "memory",
                        "rawdata.encryption.key", "password",
                        "rawdata.encryption.salt", "salt"),
                "rawdata",
                ContentStoreInitializer.class
        );
        assertNotNull(contentStore);
        HttpRequestInfo httpRequestInfo = new HttpRequestInfo(CorrelationIds.create(ExecutionContext.empty()), null, -1, new Headers(), new Headers(), -1);
        contentStore.bufferDocument("topic", "1", "entry", "PAYLOAD".getBytes(), httpRequestInfo);
        assertEquals(1, contentStore.contentKeys("topic", "1").size());
        contentStore.publish("topic", "1");
    }

    @Test
    public void thatRawdataClientProducesContent() {
        RawdataClient client = ProviderConfigurator.configure(Map.of(), "memory", RawdataClientInitializer.class);
        EncryptionClient encryptionClient = new EncryptionClient(Algorithm.AES256);
        ContentStream contentStream = new RawdataClientContentStream(client);
        ContentStreamProducer producer = contentStream.producer("ns");

        ContentStreamBuffer.Builder builder = producer.builder();
        builder.position("1")
                .buffer("a", new byte[7], null)
                .buffer("b", new byte[5], null);

        producer.produce(builder);

        producer.publish("1");
    }

    static void consumeMessages(ContentStream client) {
        try (ContentStreamConsumer consumer = client.consumer("my-rawdata-stream")) {
            for (; ; ) {
                ContentStreamBuffer message = consumer.receive(30, TimeUnit.SECONDS);
                if (message != null) {
                    System.out.printf("Consumed message with id: %s%n", message.ulid());
                    if (message.position().equals("582AACB30")) {
                        return;
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    static void produceMessages(ContentStream client) throws Exception {
        try (ContentStreamProducer producer = client.producer("my-rawdata-stream")) {
            producer.publishBuilders(producer.builder().position("4BA210EC2")
                    .put("the-payload", "Hello 1".getBytes(StandardCharsets.UTF_8)));
            producer.publishBuilders(producer.builder().position("B827B4CCE")
                    .put("the-payload", "Hello 2".getBytes(StandardCharsets.UTF_8))
                    .put("metadata", ("created-time " + System.currentTimeMillis()).getBytes(StandardCharsets.UTF_8)));
            producer.publishBuilders(producer.builder().position("582AACB30")
                    .put("the-payload", "Hello 3".getBytes(StandardCharsets.UTF_8)));
        }
    }

    @Test
    public void contentStreamConsumer() throws Exception {
        Map<String, String> configuration = Map.of(
                "content.stream.connector", "rawdata",
                "rawdata.client.provider", "memory"
        );
        ContentStore contentStore = ProviderConfigurator.configure(configuration, configuration.get("content.stream.connector"), ContentStoreInitializer.class);
        ContentStream contentStream = contentStore.contentStream();

        Thread consumerThread = new Thread(() -> consumeMessages(contentStream));
        consumerThread.start();

        produceMessages(contentStream);

        consumerThread.join();
    }
}

package no.ssb.dc.content.provider.rawdata;

import no.ssb.dc.api.CorrelationIds;
import no.ssb.dc.api.content.ContentStore;
import no.ssb.dc.api.content.ContentStoreInitializer;
import no.ssb.dc.api.content.ContentStream;
import no.ssb.dc.api.content.ContentStreamBuffer;
import no.ssb.dc.api.content.ContentStreamProducer;
import no.ssb.dc.api.content.HttpRequestInfo;
import no.ssb.dc.api.context.ExecutionContext;
import no.ssb.dc.api.http.Headers;
import no.ssb.rawdata.api.RawdataClient;
import no.ssb.rawdata.api.RawdataClientInitializer;
import no.ssb.rawdata.payload.encryption.EncryptionClient;
import no.ssb.service.provider.api.ProviderConfigurator;
import org.junit.jupiter.api.Test;

import java.util.Map;

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
    public void thatRawdataClient() {
        RawdataClient client = ProviderConfigurator.configure(Map.of(), "memory", RawdataClientInitializer.class);
        EncryptionClient encryptionClient = new EncryptionClient();
        ContentStream contentStream = new RawdataClientContentStream(client, null);
        ContentStreamProducer producer = contentStream.producer("ns");

        ContentStreamBuffer.Builder builder = producer.builder();
        builder.position("1")
                .buffer("a", new byte[7], null)
                .buffer("b", new byte[5], null);

        producer.produce(builder);

        producer.publish("1");
    }
}

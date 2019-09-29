package no.ssb.dc.content.provider.rawdata;

import no.ssb.dc.api.content.ContentStore;
import no.ssb.dc.api.content.ContentStoreInitializer;
import no.ssb.dc.api.content.ContentStream;
import no.ssb.dc.api.content.ContentStreamBuffer;
import no.ssb.dc.api.content.ContentStreamProducer;
import no.ssb.rawdata.api.RawdataClient;
import no.ssb.rawdata.api.RawdataClientInitializer;
import no.ssb.service.provider.api.ProviderConfigurator;
import org.testng.annotations.Test;

import java.util.Map;

import static org.testng.Assert.assertNotNull;

public class ContentStreamTest {

    @Test
    public void thatContentStoreInitializer() {
        ContentStore contentStore = ProviderConfigurator.configure(Map.of("rawdata.client.provider", "memory"), "rawdata", ContentStoreInitializer.class);
        assertNotNull(contentStore);
    }

    @Test
    public void thatRawdataClient() {
        RawdataClient client = ProviderConfigurator.configure(Map.of(), "memory", RawdataClientInitializer.class);
        ContentStream contentStream = new RawdataClientContentStream(client);
        ContentStreamProducer producer = contentStream.producer("ns");

        ContentStreamBuffer.Builder builder = producer.builder();
        builder.position("1")
                .buffer("a", new byte[7], null)
                .buffer("b", new byte[5], null);

        producer.produce(builder);

        producer.publish("1");
    }
}

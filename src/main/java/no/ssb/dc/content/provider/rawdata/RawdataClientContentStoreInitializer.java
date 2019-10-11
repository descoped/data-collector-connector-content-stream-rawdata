package no.ssb.dc.content.provider.rawdata;

import no.ssb.dc.api.content.ContentStore;
import no.ssb.dc.api.content.ContentStoreInitializer;
import no.ssb.rawdata.api.RawdataClient;
import no.ssb.rawdata.api.RawdataClientInitializer;
import no.ssb.service.provider.api.ProviderConfigurator;
import no.ssb.service.provider.api.ProviderName;

import java.util.Map;
import java.util.Set;

@ProviderName("rawdata")
public class RawdataClientContentStoreInitializer implements ContentStoreInitializer {

    @Override
    public String providerId() {
        return "rawdata";
    }

    @Override
    public Set<String> configurationKeys() {
        return Set.of("rawdata.client.provider");
    }

    @Override
    public ContentStore initialize(Map<String, String> configuration) {
        RawdataClient client = ProviderConfigurator.configure(configuration, configuration.get("rawdata.client.provider"), RawdataClientInitializer.class);
        return new RawdataClientContentStore(client);
    }
}

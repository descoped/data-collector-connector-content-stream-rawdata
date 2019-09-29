package no.ssb.dc.content.provider.rawdata;

import no.ssb.dc.api.content.ContentStore;
import no.ssb.dc.api.content.ContentStoreInitializer;
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
        return new RawdataClientContentStore(configuration);
    }
}

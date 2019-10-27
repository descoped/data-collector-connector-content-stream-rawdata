package no.ssb.dc.content.provider.rawdata;

import no.ssb.dc.api.content.ContentStore;
import no.ssb.dc.api.content.ContentStoreInitializer;
import no.ssb.rawdata.api.RawdataClient;
import no.ssb.rawdata.api.RawdataClientInitializer;
import no.ssb.service.provider.api.ProviderConfigurator;
import no.ssb.service.provider.api.ProviderName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;

@ProviderName("rawdata")
public class RawdataClientContentStoreInitializer implements ContentStoreInitializer {

    private static final Logger LOG = LoggerFactory.getLogger(RawdataClientContentStoreInitializer.class);

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
        LOG.info("Content stream connector: {}", configuration.get("content.stream.connector"));
        LOG.info("Rawdata client provider: {}", configuration.get("rawdata.client.provider"));
        RawdataClient client = ProviderConfigurator.configure(configuration, configuration.get("rawdata.client.provider"), RawdataClientInitializer.class);
        return new RawdataClientContentStore(client);
    }
}

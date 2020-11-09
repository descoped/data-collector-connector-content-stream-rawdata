package no.ssb.dc.content.provider.rawdata;

import no.ssb.dapla.secrets.api.SecretManagerClient;
import no.ssb.dc.api.content.ContentStore;
import no.ssb.dc.api.content.ContentStoreInitializer;
import no.ssb.rawdata.api.RawdataClient;
import no.ssb.rawdata.api.RawdataClientInitializer;
import no.ssb.service.provider.api.ProviderConfigurator;
import no.ssb.service.provider.api.ProviderName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static no.ssb.dapla.secrets.api.SecretManagerClient.safeCharArrayAsUTF8;

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

    /*
     * Google SecretManager config properties:
     *
     * rawdata.encryption.provider = (dynamic-secret-configuration | google-secret-manager)
     * rawdata.encryption.gcp.projectId => google-secret-manager
     * rawdata.encryption.gcp.serviceAccountKeyPath => google-secret-manager
     * rawdata.encryption.key  = (configuration key | secret manager encryptionKey)
     * rawdata.encryption.salt = (configuration salt | secret manager encryptionKey)
     */

    @Override
    public ContentStore initialize(Map<String, String> configuration) {
        LOG.info("Content stream connector: {}", configuration.get("content.stream.connector"));

        LOG.info("Rawdata client provider: {}", configuration.get("rawdata.client.provider"));
        LOG.info("Rawdata encryption enabled: {}", configuration.containsKey("rawdata.encryption.key"));

        // get encryption provider
        String encryptionProvider = configuration.get("rawdata.encryption.provider");
        Map<String, String> encryptionProviderMap = new LinkedHashMap<>();

        // load encryption keys from custom config file
        if ("dynamic-secret-configuration".equals(encryptionProvider)) {
            String gcpServiceAccountKeyPath = configuration.get("rawdata.encryption.gcp.serviceAccountKeyPath");
            if (gcpServiceAccountKeyPath == null) {
                LOG.info("Use custom encryption credentials configuration: {}", gcpServiceAccountKeyPath);
                encryptionProviderMap.putAll(configuration);
            } else {
                LOG.info("Use application configuration for encryption credentials");
            }
            encryptionProviderMap.put("secrets.provider", encryptionProvider);
            if (gcpServiceAccountKeyPath != null) {
                encryptionProviderMap.put("secrets.propertyResourcePath", gcpServiceAccountKeyPath);
            }

        // get encryption keys from google secret manager
        } else if ("google-secret-manager".equals(encryptionProvider)) {
            LOG.info("Use Google Secret Manager for encryption credentials");
            encryptionProviderMap.put("secrets.provider", encryptionProvider);
            encryptionProviderMap.put("secrets.projectId", Optional.ofNullable(configuration.get("rawdata.encryption.gcp.projectId")).orElseThrow());
            // fallback to compute-engine if service-account-file is not set
            String gcpServiceAccountFile = configuration.get("rawdata.encryption.gcp.serviceAccountKeyPath");
            if (gcpServiceAccountFile != null) {
                encryptionProviderMap.put("secrets.serviceAccountKeyPath", gcpServiceAccountFile);
            }

        // fallback: load encryption keys from current configuration
        } else if (configuration.containsKey("rawdata.encryption.key") && configuration.containsKey("rawdata.encryption.salt")) {
            LOG.info("Fallback application configuration for encryption credentials");
            encryptionProviderMap.putAll(configuration);
            encryptionProviderMap.put("secrets.provider", "dynamic-secret-configuration");
        }

        if (encryptionProviderMap.isEmpty()) {
            RawdataClient client = ProviderConfigurator.configure(configuration, configuration.get("rawdata.client.provider"), RawdataClientInitializer.class);
            return new RawdataClientContentStore(client, null, null);

        } else {
            LOG.debug("Load encryption credentials from: {}", encryptionProvider == null ? "application configuration" : encryptionProvider);
            char[] encryptionKeySecretValue = new char[0];
            byte[] encryptionSaltSecretValue = new byte[0];
            try (SecretManagerClient secretManagerClient = SecretManagerClient.create(encryptionProviderMap)) {
                if ("google-secret-manager".equals(encryptionProvider)) {
                    LOG.debug("Has: rawdata.encryption.key='{}'", configuration.containsKey("rawdata.encryption.key"));
                    LOG.debug("Has: rawdata.encryption.salt='{}'", configuration.containsKey("rawdata.encryption.salt"));
                }

                String encryptionKeySecretName = configuration.containsKey("rawdata.encryption.key") && "google-secret-manager".equals(encryptionProvider) ?
                        Optional.ofNullable(configuration.get("rawdata.encryption.key")).orElseThrow() :
                        "rawdata.encryption.key";

                String encryptionSaltSecretName = configuration.containsKey("rawdata.encryption.salt") && "google-secret-manager".equals(encryptionProvider) ?
                        Optional.ofNullable(configuration.get("rawdata.encryption.salt")).orElseThrow() :
                        "rawdata.encryption.salt";

                if ("google-secret-manager".equals(encryptionProvider)) {
                    LOG.debug("rawdata.encryption.key='{}'", encryptionKeySecretName);
                    LOG.debug("rawdata.encryption.salt='{}'", encryptionSaltSecretName);
                }

                encryptionKeySecretValue = configuration.containsKey("rawdata.encryption.key") ? safeCharArrayAsUTF8(secretManagerClient.readBytes(encryptionKeySecretName)) : null;
                encryptionSaltSecretValue = configuration.containsKey("rawdata.encryption.salt") ? secretManagerClient.readBytes(encryptionSaltSecretName) : null;

                RawdataClient client = ProviderConfigurator.configure(configuration, configuration.get("rawdata.client.provider"), RawdataClientInitializer.class);
                return new RawdataClientContentStore(client, encryptionKeySecretValue, encryptionSaltSecretValue);

            } finally {
                Arrays.fill(encryptionKeySecretValue, (char) 0);
                Arrays.fill(encryptionSaltSecretValue, (byte) 0);
            }
        }
    }
}

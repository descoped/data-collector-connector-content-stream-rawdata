import io.descoped.dc.api.content.ContentStoreInitializer;

module io.descoped.dc.content.rawdata {

    requires io.descoped.service.provider.api;
    requires io.descoped.rawdata.api;
    requires io.descoped.dc.api;
    requires io.descoped.encryption.util;
    requires io.descoped.secrets.client.api;

    requires org.slf4j;
    requires de.huxhorn.sulky.ulid;
    requires com.fasterxml.jackson.databind;
    requires com.fasterxml.jackson.annotation;

    provides ContentStoreInitializer with io.descoped.dc.content.provider.rawdata.RawdataClientContentStoreInitializer;

    exports io.descoped.dc.content.provider.rawdata;
}

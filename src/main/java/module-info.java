import no.ssb.dc.api.content.ContentStoreInitializer;

module no.ssb.dc.content.rawdata {

    requires no.ssb.service.provider.api;
    requires no.ssb.rawdata.api;
    requires no.ssb.dc.api;
    requires io.descoped.encryption.util;
    requires dapla.secrets.client.api;

    requires org.slf4j;
    requires de.huxhorn.sulky.ulid;
    requires com.fasterxml.jackson.databind;
    requires com.fasterxml.jackson.annotation;

    provides ContentStoreInitializer with no.ssb.dc.content.provider.rawdata.RawdataClientContentStoreInitializer;

    exports no.ssb.dc.content.provider.rawdata;
}

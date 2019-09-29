import no.ssb.dc.api.content.ContentStoreInitializer;
import no.ssb.dc.content.provider.rawdata.RawdataClientContentStoreInitializer;

module no.ssb.dc.content.rawdata {

    requires no.ssb.service.provider.api;
    requires no.ssb.rawdata.api;
    requires no.ssb.dc.api;
    requires de.huxhorn.sulky.ulid;
    requires com.fasterxml.jackson.core;
    requires com.fasterxml.jackson.databind;
    requires com.fasterxml.jackson.annotation;

    provides ContentStoreInitializer with RawdataClientContentStoreInitializer;

    exports no.ssb.dc.content.provider.rawdata;
}

package no.ssb.dc.content.provider.rawdata;

import de.huxhorn.sulky.ulid.ULID;
import no.ssb.dc.api.content.ContentStreamBuffer;
import no.ssb.dc.api.content.MetadataContent;
import no.ssb.rawdata.api.RawdataMessage;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RawdataClientContentStreamBuffer implements ContentStreamBuffer {

    private final ULID.Value ulid;
    private final String position;
    private final Map<String, byte[]> data;
    private final List<MetadataContent> manifestList;

    RawdataClientContentStreamBuffer(ULID.Value ulid, String position, Map<String, byte[]> data, List<MetadataContent> manifestList) {
        this.ulid = ulid;
        this.position = position;
        this.data = data;
        this.manifestList = manifestList;
    }

    public static ContentStreamBuffer of(RawdataMessage message) {
        return new RawdataClientContentStreamBuffer(message.ulid(), message.position(), message.data(), new ArrayList<>());
    }

    @Override
    public ULID.Value ulid() {
        return ulid;
    }

    @Override
    public String position() {
        return position;
    }

    @Override
    public Set<String> keys() {
        return data.keySet();
    }

    @Override
    public Map<String, byte[]> data() {
        return data;
    }

    @Override
    public List<MetadataContent> manifest() {
        return manifestList;
    }

    public static class Builder implements ContentStreamBuffer.Builder {

        private ULID.Value ulid;
        private String position;
        final Map<String, byte[]> data = new LinkedHashMap<>();
        private List<MetadataContent> manifestList = new ArrayList<>();

        @Override
        public ContentStreamBuffer.Builder ulid(ULID.Value ulid) {
            this.ulid = ulid;
            return this;
        }

        @Override
        public ContentStreamBuffer.Builder position(String position) {
            this.position = position;
            return this;
        }

        @Override
        public String position() {
            return position;
        }

        @Override
        public ContentStreamBuffer.Builder put(String key, byte[] payload) {
            data.put(key, payload);
            return this;
        }

        @Override
        public Builder buffer(String contentKey, byte[] content, MetadataContent manifest) {
            data.put(contentKey, content);
            if (manifest != null) {
                manifestList.add(manifest);
            }
            return this;
        }

        @Override
        public byte[] get(String contentKey) {
            return data.get(contentKey);
        }

        @Override
        public Set<String> keys() {
            return data.keySet();
        }

        @Override
        public List<MetadataContent> manifest() {
            return manifestList;
        }

        @Override
        public ContentStreamBuffer build() {
            return new RawdataClientContentStreamBuffer(ulid, position, data, manifestList);
        }

    }
}

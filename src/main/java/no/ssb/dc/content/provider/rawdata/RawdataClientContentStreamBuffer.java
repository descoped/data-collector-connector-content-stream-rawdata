package no.ssb.dc.content.provider.rawdata;

import no.ssb.dc.api.content.ContentStreamBuffer;
import no.ssb.dc.api.content.MetadataContent;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RawdataClientContentStreamBuffer implements ContentStreamBuffer {

    private final String position;
    private final Map<String, byte[]> data;
    private final List<MetadataContent> manifestList;

    RawdataClientContentStreamBuffer(String position, Map<String, byte[]> data, List<MetadataContent> manifestList) {
        this.position = position;
        this.data = data;
        this.manifestList = manifestList;
    }

    @Override
    public String position() {
        return position;
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

        private String position;
        final Map<String, byte[]> data = new LinkedHashMap<>();
        private List<MetadataContent> manifestList = new ArrayList<>();

        @Override
        public ContentStreamBuffer.Builder position(String position) {
            this.position = position;
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
        public Set<String> keys() {
            return data.keySet();
        }

        @Override
        public List<MetadataContent> manifest() {
            return manifestList;
        }

        @Override
        public ContentStreamBuffer build() {
            return new RawdataClientContentStreamBuffer(position, data, manifestList);
        }

    }
}

package no.ssb.dc.content.provider.rawdata;

import no.ssb.dc.api.content.ClosedContentStreamException;
import no.ssb.dc.api.content.ContentStreamBuffer;
import no.ssb.dc.api.content.ContentStreamConsumer;
import no.ssb.rawdata.api.RawdataConsumer;

import java.util.concurrent.TimeUnit;

public class RawdataContentStreamConsumer implements ContentStreamConsumer {

    private final RawdataConsumer consumer;

    public RawdataContentStreamConsumer(RawdataConsumer consumer) {
        this.consumer = consumer;
    }

    @Override
    public String topic() {
        return consumer.topic();
    }

    @Override
    public ContentStreamBuffer receive(int timeout, TimeUnit unit) throws InterruptedException, ClosedContentStreamException {
        if (isClosed()) {
            throw new ClosedContentStreamException();
        }
        return RawdataClientContentStreamBuffer.of(consumer.receive(timeout, unit));
    }

    @Override
    public void seek(long timestamp) {
        consumer.seek(timestamp);
    }

    @Override
    public boolean isClosed() {
        return consumer.isClosed();
    }

    @Override
    public void close() throws Exception {
        consumer.close();
    }
}

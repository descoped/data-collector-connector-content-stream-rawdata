package no.ssb.dc.content.provider.rawdata;

import no.ssb.dc.api.content.ClosedContentStreamException;
import no.ssb.dc.api.content.ContentStreamBuffer;
import no.ssb.dc.api.content.ContentStreamConsumer;
import no.ssb.rawdata.api.RawdataConsumer;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class RawdataClientContentStreamConsumer implements ContentStreamConsumer {

    private final RawdataConsumer consumer;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public RawdataClientContentStreamConsumer(RawdataConsumer consumer) {
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
        if (isClosed()) {
            throw new ClosedContentStreamException();
        }
        consumer.seek(timestamp);
    }

    @Override
    public boolean isClosed() {
        return consumer.isClosed();
    }

    @Override
    public void close() throws Exception {
        if (closed.compareAndSet(false, true)) {
            consumer.close();
        }
    }
}

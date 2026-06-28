package io.github.springtxnkafka.outbox.relay;

public interface OutboxRelay {

    void poll();
}

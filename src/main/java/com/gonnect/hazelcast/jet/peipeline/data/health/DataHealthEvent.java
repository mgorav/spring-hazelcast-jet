package com.gonnect.hazelcast.jet.peipeline.data.health;

import java.io.Serializable;

/**
 * We use java.io.{@link java.io.Serializable} here for the sake of simplicity.
 * In production, Hazelcast Custom Serialization should be used.
 */
public class DataHealthEvent implements Serializable {

    private final Type type;
    private final long transactionId;
    private final long timestamp;

    public enum Type {
        START, END
    }

    public DataHealthEvent(long timestamp, long transactionId, Type type) {
        this.timestamp = timestamp;
        this.transactionId = transactionId;
        this.type = type;
    }

    public Type type() {
        return type;
    }

    public long transactionId() {
        return transactionId;
    }

    public long timestamp() {
        return timestamp;
    }
}
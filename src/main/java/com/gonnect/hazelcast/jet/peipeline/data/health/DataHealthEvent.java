package com.gonnect.hazelcast.jet.peipeline.data.health;

import java.io.Serializable;

/**
 * We use java.io.{@link Serializable} here for the sake of simplicity.
 * In production, Hazelcast Custom Serialization should be used.
 */
public class DataHealthEvent implements Serializable {

    private final Type type;
    private final long enrollmentId; // parent
//    private final long contributeId; //child
    private final long timestamp;
    private Double score;

    public enum Type {
        START, ERROR,END
    }
    public enum Category {
        SIGNAL, EVENT
    }

    public DataHealthEvent(long timestamp, long enrollmentId, Type type) {
        this.timestamp = timestamp;
        this.enrollmentId = enrollmentId;
        this.type = type;
    }

    public Type type() {
        return type;
    }

    public long transactionId() {
        return enrollmentId;
    }

    public long timestamp() {
        return timestamp;
    }
}

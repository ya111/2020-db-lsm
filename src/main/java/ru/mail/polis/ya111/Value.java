package ru.mail.polis.ya111;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

public final class Value implements Comparable<Value> {
    private final long timestamp;
    private final ByteBuffer data;

    /**
     * Create value from ByteBuffer
     * @param timestamp time of creation
     * @param data buffer to get data value
     */
    public Value(final long timestamp, final ByteBuffer data) {
        assert timestamp >= 0L;
        this.timestamp = timestamp;
        this.data = data;
    }

    public static Value tombstone(final long time) {
        return new Value(time, null);
    }

    public boolean isRemoved() {
        return data == null;
    }

    @NotNull
    ByteBuffer getData() {
        assert !isRemoved();
        return data.asReadOnlyBuffer();
    }

    @Override
    public int compareTo(@NotNull final Value o) {
        return -Long.compare(this.timestamp, o.timestamp);
    }

    public long getTimestamp() {
        return timestamp;
    }
}

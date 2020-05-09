package ru.mail.polis.ya111;

import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;

public class MemTable implements Table {
    private final SortedMap<ByteBuffer, Value> map = new TreeMap<>();
    private long sizeInBytes;

    @NotNull
    @Override
    public Iterator<Cell> iterator(@NotNull final ByteBuffer from) throws IOException {
        return Iterators.transform(map.tailMap(from).entrySet().iterator(),
                entry -> new Cell(entry.getKey(), entry.getValue()));
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) throws IOException {
        final Value previous = map.put(key, new Value(System.currentTimeMillis(), value));
        if (previous == null) {
            sizeInBytes += key.remaining() + value.remaining();
        } else if (previous.isRemoved()) {
            sizeInBytes += value.remaining();
        } else {
            sizeInBytes += value.remaining() - previous.getData().remaining();
        }
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        final Value previous = map.put(key, Value.tombstone(System.currentTimeMillis()));
        if (previous == null) {
            sizeInBytes += key.remaining();
        } else if (!previous.isRemoved()) {
            sizeInBytes -= previous.getData().remaining();
        }
    }

    @Override
    public long sizeInBytes() {
        return sizeInBytes;
    }

    @Override
    public int size() {
        return map.size();
    }

    @Override
    public void close() {
        //nothing to close
    }
}

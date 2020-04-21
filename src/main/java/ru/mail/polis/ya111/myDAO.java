package ru.mail.polis.ya111;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.DAO;
import ru.mail.polis.Record;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;

public class myDAO implements DAO {

    private final SortedMap<ByteBuffer, Record> data = new TreeMap<>();

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) {
        return data.tailMap(from)
                .values()
                .iterator();
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) {
        data.put(key, Record.of(key, value));
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) {
        data.remove(key);
    }

    @Override
    public void close() {
        //TODO
    }
}
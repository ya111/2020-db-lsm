package ru.mail.polis.ya111;

import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.DAO;
import ru.mail.polis.Iters;
import ru.mail.polis.Record;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.stream.Stream;

public class DAOimpl implements DAO {
    private static final Logger log = LoggerFactory.getLogger(DAOimpl.class);

    private static String SUFFIX = ".dat";
    private static String TEMP = ".tmp";

    @NotNull
    private final File storage;
    private final long flushThreshold;

    // Data
    private Table memTable;
    private final NavigableMap<Integer, Table> ssTables;

    // State
    private int generation;

    /**
     * Created LsmDao from storage with limit = flushThreshold.
     */
    public DAOimpl(@NotNull final File storage, final long flushThreshold) throws IOException {
        assert flushThreshold > 0L;
        this.storage = storage;
        this.flushThreshold = flushThreshold;
        this.memTable = new MemTable();
        this.ssTables = new TreeMap<>();
        try (Stream<Path> files = Files.list(storage.toPath())) {
            files.filter(path -> path.toString().endsWith(SUFFIX)).forEach(f -> {
                try {
                    // 3.dat
                    final String name = f.getFileName().toString();
                    final int gen = Integer.parseInt(name.substring(0, name.indexOf(SUFFIX)));
                    generation = Math.max(generation, gen);
                    ssTables.put(gen, new SSTable(f.toFile()));
                } catch (IOException e) {
                    // Log bad file
                    log.info("IOException in 'new SSTable'");
                } catch (NumberFormatException e) {
                    log.info("Incorrect file name");
                }
            });
        }
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) throws IOException {
        final List<Iterator<Cell>> iters = new ArrayList<>(ssTables.size() + 1);
        iters.add(memTable.iterator(from));
        for (final Table t : ssTables.descendingMap().values()) {
            iters.add(t.iterator(from));
        }
        // Sorted duplicates and tombstones
        final Iterator<Cell> merged = Iterators.mergeSorted(iters, Cell.COMPARATOR);
        // One cell per key
        final Iterator<Cell> fresh = Iters.collapseEquals(merged, Cell::getKey);
        // No tombstones
        final Iterator<Cell> alive = Iterators.filter(fresh, e -> !e.getValue().isRemoved());
        return Iterators.transform(alive, e -> Record.of(e.getKey(), e.getValue().getData()));
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) throws IOException {
        memTable.upsert(key, value);
        if (memTable.sizeInBytes() > flushThreshold) {
            flush();
        }
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        memTable.remove(key);
        if (memTable.sizeInBytes() > flushThreshold) {
            flush();
        }
    }

    private void flush() throws IOException {
        // Dump memTable
        final File file = new File(storage, generation + TEMP);
        SSTable.serialize(file, memTable.iterator(ByteBuffer.allocate(0)), memTable.size());
        final File dst = new File(storage, generation + SUFFIX);
        Files.move(file.toPath(), dst.toPath(), StandardCopyOption.ATOMIC_MOVE);

        // Switch
        memTable = new MemTable();
        ssTables.put(generation, new SSTable(dst));
        generation++;
    }

    @Override
    public void close() throws IOException {
        if (memTable.size() > 0) {
            flush();
        }

        for (final Table t : ssTables.values()) {
            t.close();
        }
    }
}
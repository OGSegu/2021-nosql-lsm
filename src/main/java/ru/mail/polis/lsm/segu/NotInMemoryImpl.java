package ru.mail.polis.lsm.segu;

import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.DAOConfig;
import ru.mail.polis.lsm.Record;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Implementation of NotInMemory DAO.
 */

public class NotInMemoryImpl implements DAO {
    private final SortedMap<ByteBuffer, ValueHolder<Record>> storage = new ConcurrentSkipListMap<>();

    private final DAOConfig config;
    private static final String FILE_NAME = "data.dat";
    private Path filePath;

    /**
     * Constructor.
     *
     * @param config - конфиг
     */

    public NotInMemoryImpl(DAOConfig config) throws IOException {
        this.config = config;
        initStorage();
    }

    private void initStorage() throws IOException {
        filePath = config.getDir().resolve(FILE_NAME);
        if (!Files.exists(filePath)) {
            return;
        }
        try (FileChannel fileChannel = FileChannel.open(filePath, StandardOpenOption.READ)) {
            final ByteBuffer size = ByteBuffer.allocate(Integer.BYTES);
            while (fileChannel.read(size) > 0) {
                final ByteBuffer key = readValue(fileChannel, size);
                fileChannel.read(size.flip());
                size.position(0);
                if (size.getInt() < 0) {
                    storage.put(key, new ValueHolder<>(() ->
                            Record.tombstone(key)
                    ));
                } else {
                    storage.put(key, new ValueHolder<>(() -> {
                        ByteBuffer value = readValueByKey(key);
                        if (value == null) {
                            return Record.tombstone(key);
                        } else {
                            return Record.of(key, value);
                        }
                    }));
                }
            }
        }
    }

    @Override
    public Iterator<Record> range(@Nullable final ByteBuffer fromKey, @Nullable final ByteBuffer toKey) {
        return map(fromKey, toKey).values().stream()
                .map(ValueHolder::getValue)
                .filter(record -> record.getValue() != null)
                .iterator();
    }

    @Override
    public void upsert(final Record record) {
        storage.put(record.getKey(), new ValueHolder<>(() -> record));
    }

    @Override
    public void close() throws IOException {
        try (FileChannel fileChannel = FileChannel.open(filePath,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE)) {
            final ByteBuffer size = ByteBuffer.allocate(Integer.BYTES);
            for (final ValueHolder<Record> valueHolderRecord : storage.values()) {
                Record value = valueHolderRecord.getValue();
                writeValue(fileChannel, value.getKey(), size);
                writeValue(fileChannel, value.getValue(), size);
            }
        }
    }

    private void writeValue(FileChannel fileChannel, @Nullable ByteBuffer value, ByteBuffer size) throws IOException {
        if (value == null) {
            saveAndWriteSize(fileChannel, size, -1);
        } else {
            saveAndWriteSize(fileChannel, size, value.remaining());
            fileChannel.write(value);
        }
    }

    private void saveAndWriteSize(FileChannel fileChannel, ByteBuffer sizeBuffer, int size) throws IOException {
        sizeBuffer.position(0);
        sizeBuffer.putInt(size);
        sizeBuffer.position(0);
        fileChannel.write(sizeBuffer);
    }

    private ByteBuffer readValue(FileChannel fileChannel, ByteBuffer size) throws IOException {
        size.flip();
        final ByteBuffer value = ByteBuffer.allocate(size.getInt());
        fileChannel.read(value);
        return value.flip();
    }


    private ByteBuffer readValueByKey(ByteBuffer key) {
        final ByteBuffer sizeBuffer = ByteBuffer.allocateDirect(Integer.BYTES);
        try (FileChannel fileChannel = FileChannel.open(filePath, StandardOpenOption.READ)) {
            while (fileChannel.read(sizeBuffer) > 0) {
                sizeBuffer.position(0);
                int keySize = sizeBuffer.getInt();
                sizeBuffer.position(0);

                ByteBuffer valueBuffer = ByteBuffer.allocateDirect(keySize);
                fileChannel.read(valueBuffer);
                valueBuffer.position(0);

                sizeBuffer.position(0);
                fileChannel.read(sizeBuffer);
                sizeBuffer.position(0);
                int valueSize = sizeBuffer.getInt();

                if (valueBuffer.equals(key)) {
                    valueBuffer = ByteBuffer.allocateDirect(valueSize);
                    fileChannel.read(valueBuffer);
                    valueBuffer.position(0);
                    return valueBuffer;
                } else {
                    if (valueSize == -1) {
                        continue;
                    }
                    valueBuffer = ByteBuffer.allocateDirect(valueSize);
                    fileChannel.read(valueBuffer);
                    valueBuffer.position(0);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private SortedMap<ByteBuffer, ValueHolder<Record>> map(@Nullable final ByteBuffer fromKey, @Nullable final ByteBuffer toKey) {
        if (fromKey == null && toKey == null) {
            return storage;
        }
        if (fromKey == null) {
            return storage.headMap(toKey);
        }
        if (toKey == null) {
            return storage.tailMap(fromKey);
        }
        return storage.subMap(fromKey, toKey);
    }
}

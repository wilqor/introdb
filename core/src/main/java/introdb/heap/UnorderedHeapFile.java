package introdb.heap;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

class UnorderedHeapFile implements Store {

    private final Path path;
    private final int pageSize;
    private final int maxNrPages;

    UnorderedHeapFile(Path path, int maxNrPages, int pageSize) {
        this.path = path;
        this.maxNrPages = maxNrPages;
        this.pageSize = pageSize;
    }

    @Override
    public void put(Entry entry) throws IOException, ClassNotFoundException {
        remove(entry.key());
        try (FileChannel fileChannel = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ)) {
            long currentSize = fileChannel.size();
            long fullPagesCount = currentSize / pageSize;
            long remainingPageSize = currentSize % pageSize;
            long totalPagesCount = fullPagesCount + (remainingPageSize > 0 ? 1 : 0);
            var record = EntryRecord.fromEntry(entry);
            if (record.getRecordSize() > pageSize) {
                throw new IllegalArgumentException("Too large object!");
            }
            long pageStart;
            if ((pageSize - remainingPageSize) >= record.getRecordSize()) {
                // wrote on last page
                pageStart = fullPagesCount * pageSize;
            } else {
                // append new page
                pageStart = totalPagesCount * pageSize;
                if (totalPagesCount == maxNrPages) {
                    throw new IllegalArgumentException("Max number of pages reached!");
                }
            }
            ByteBuffer byteBuffer = ByteBuffer.allocate(pageSize);
            fileChannel.read(byteBuffer, pageStart);
            record.writeToBuffer(byteBuffer);
            byteBuffer.flip();
            fileChannel.write(byteBuffer, pageStart);
        }
    }

    @Override
    public Object get(Serializable key) throws IOException, ClassNotFoundException {
        try (FileChannel fileChannel = FileChannel.open(path)) {
            ByteBuffer byteBuffer = ByteBuffer.allocate(pageSize);
            while (fileChannel.read(byteBuffer) > 0) {
                // flip for reading
                byteBuffer.flip();
                while (byteBuffer.hasRemaining()) {
                    var entryRecord = EntryRecord.fromBuffer(byteBuffer);
                    if (entryRecord == null) {
                        break;
                    }
                    if (entryRecord.isNotDeleted() && entryRecord.getEntry().key().equals(key)) {
                        return entryRecord.getEntry().value();
                    }
                }
                byteBuffer.clear();
            }
            return null;
        }
    }

    @Override
    public Object remove(Serializable key) throws IOException, ClassNotFoundException {
        try (FileChannel fileChannel = FileChannel.open(path, StandardOpenOption.WRITE, StandardOpenOption.READ)) {
            ByteBuffer byteBuffer = ByteBuffer.allocate(pageSize);
            while (fileChannel.read(byteBuffer) > 0) {
                // flip for reading
                byteBuffer.flip();
                while (byteBuffer.hasRemaining()) {
                    var entryRecord = EntryRecord.fromBuffer(byteBuffer);
                    if (entryRecord == null) {
                        break;
                    }
                    if (entryRecord.isNotDeleted() && key.equals(entryRecord.getEntry().key())) {
                        var deleted = entryRecord.toDeleted();
                        byteBuffer.position(byteBuffer.position() - deleted.getRecordSize());
                        deleted.writeToBuffer(byteBuffer);
                        // flip for writing
                        byteBuffer.flip();
                        long writePosition = fileChannel.position() - byteBuffer.limit();
                        // fill entire page
                        byteBuffer.limit(pageSize);
                        fileChannel.write(byteBuffer, writePosition);
                        return deleted.getEntry().value();
                    }
                }
                byteBuffer.clear();
            }
            return null;
        }
    }

}


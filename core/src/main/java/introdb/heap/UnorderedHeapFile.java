package introdb.heap;

import java.io.IOException;
import java.io.Serializable;
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
            var record = EntryRecord.fromEntry(entry);
            EntryPage.findPageForAppending(pageSize, maxNrPages, fileChannel, record.getRecordSize())
                    .writeRecordAtCurrentPosition(record);
        }
    }

    @Override
    public Object get(Serializable key) throws IOException, ClassNotFoundException {
        try (FileChannel fileChannel = FileChannel.open(path)) {
            EntryPage nextPage;
            while ((nextPage = EntryPage.findNextPage(pageSize, fileChannel)) != null) {
                EntryRecord entryRecord = nextPage.searchForRecord(key);
                if (entryRecord != null) {
                    return entryRecord.getEntry().value();
                }
            }
            return null;
        }
    }

    @Override
    public Object remove(Serializable key) throws IOException, ClassNotFoundException {
        try (FileChannel fileChannel = FileChannel.open(path, StandardOpenOption.WRITE, StandardOpenOption.READ)) {
            EntryPage nextPage;
            while ((nextPage = EntryPage.findNextPage(pageSize, fileChannel)) != null) {
                var entryRecord = nextPage.searchForRecord(key);
                if (entryRecord != null) {
                    nextPage.deleteLastFoundRecord(entryRecord);
                    return entryRecord.getEntry().value();
                }
            }
            return null;
        }
    }

}


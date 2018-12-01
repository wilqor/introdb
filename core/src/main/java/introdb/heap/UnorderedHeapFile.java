package introdb.heap;

import java.io.IOException;
import java.io.Serializable;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

class UnorderedHeapFile implements Store {
    private final Path path;
    private final PageProvider pageProvider;

    UnorderedHeapFile(Path path, int maxNrPages, int pageSize) {
        this.path = path;
        this.pageProvider = new PageProvider(pageSize, maxNrPages);
    }

    @Override
    public void put(Entry entry) throws IOException, ClassNotFoundException {
        try (FileChannel fileChannel = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ)) {
            findAndDeleteRecord(fileChannel, entry.key());
            var record = EntryRecord.fromEntry(entry);
            pageProvider.pageForAppending(fileChannel, record.recordSize())
                    .appendRecord(record);
        }
    }

    @Override
    public Object get(Serializable key) throws IOException, ClassNotFoundException {
        try (FileChannel fileChannel = FileChannel.open(path)) {
            var pageWithRecord = findPageWithRecord(fileChannel, key);
            if (pageWithRecord != null) {
                return pageWithRecord.getRecord().entry().value();
            }
            return null;
        }
    }

    @Override
    public Object remove(Serializable key) throws IOException, ClassNotFoundException {
        try (FileChannel fileChannel = FileChannel.open(path, StandardOpenOption.WRITE, StandardOpenOption.READ)) {
            var pageWithRecord = findAndDeleteRecord(fileChannel, key);
            if (pageWithRecord != null) {
                return pageWithRecord.getRecord().entry().value();
            }
            return null;
        }
    }

    private PageWithRecord findAndDeleteRecord(FileChannel fileChannel, Serializable key) throws IOException, ClassNotFoundException {
        var pageWithRecord = findPageWithRecord(fileChannel, key);
        if (pageWithRecord != null) {
            pageWithRecord.getPage().deleteLastFoundRecord(pageWithRecord.getRecord());
        }
        return pageWithRecord;
    }

    private PageWithRecord findPageWithRecord(FileChannel fileChannel, Serializable key) throws IOException, ClassNotFoundException {
        var pageIterator = pageProvider.iterator(fileChannel);
        while (pageIterator.hasNext()) {
            var page = pageIterator.next();
            var entryRecord = page.searchForRecord(key);
            if (entryRecord != null) {
                return new PageWithRecord(page, entryRecord);
            }
        }
        return null;
    }

}


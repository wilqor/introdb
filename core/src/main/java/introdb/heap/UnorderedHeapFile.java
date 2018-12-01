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
        try (FileChannel fileChannel = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ)) {
            findAndDeleteRecord(fileChannel, entry.key());
            var record = EntryRecord.fromEntry(entry);
            EntryPage.findPageForAppending(pageSize, maxNrPages, fileChannel, record.getRecordSize())
                    .writeRecordAtCurrentPosition(record);
        }
    }

    @Override
    public Object get(Serializable key) throws IOException, ClassNotFoundException {
        try (FileChannel fileChannel = FileChannel.open(path)) {
            var pageWithRecord = findPageWithRecord(fileChannel, key);
            if (pageWithRecord != null) {
                return pageWithRecord.getRecord().getEntry().value();
            }
            return null;
        }
    }

    @Override
    public Object remove(Serializable key) throws IOException, ClassNotFoundException {
        try (FileChannel fileChannel = FileChannel.open(path, StandardOpenOption.WRITE, StandardOpenOption.READ)) {
            var pageWithRecord = findAndDeleteRecord(fileChannel, key);
            if (pageWithRecord != null) {
                return pageWithRecord.getRecord().getEntry().value();
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
        EntryPage nextPage;
        while ((nextPage = EntryPage.findNextPage(pageSize, fileChannel)) != null) {
            var entryRecord = nextPage.searchForRecord(key);
            if (entryRecord != null) {
                return new PageWithRecord(nextPage, entryRecord);
            }
        }
        return null;
    }

}


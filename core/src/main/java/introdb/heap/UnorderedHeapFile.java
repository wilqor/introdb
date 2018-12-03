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
        this.pageProvider = new PageProvider(maxNrPages, pageSize);
    }

    @Override
    public void put(Entry entry) throws IOException {
        try (FileChannel fileChannel = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ)) {
            var record = EntryRecord.fromEntry(entry);
            pageProvider.pageForAppending(fileChannel, record.recordSize())
                    .append(record);
        }
    }

    @Override
    public Object get(Serializable key) throws IOException, ClassNotFoundException {
        try (FileChannel fileChannel = FileChannel.open(path, StandardOpenOption.READ)) {
            var pageWithRecord = findPageWithRecord(fileChannel, key);
            if (pageWithRecord != null) {
                return pageWithRecord.record().entry().value();
            }
            return null;
        }
    }

    @Override
    public Object remove(Serializable key) throws IOException, ClassNotFoundException {
        try (FileChannel fileChannel = FileChannel.open(path, StandardOpenOption.WRITE, StandardOpenOption.READ)) {
            var pageWithRecord = findAndDeleteRecord(fileChannel, key);
            if (pageWithRecord != null) {
                return pageWithRecord.record().entry().value();
            }
            return null;
        }
    }

    private PageWithRecord findAndDeleteRecord(FileChannel fileChannel, Serializable key) throws IOException, ClassNotFoundException {
        var pageWithRecord = findPageWithRecord(fileChannel, key);
        if (pageWithRecord != null) {
            pageWithRecord.page().delete(pageWithRecord.record());
        }
        return pageWithRecord;
    }

    private PageWithRecord findPageWithRecord(FileChannel fileChannel, Serializable key) throws IOException, ClassNotFoundException {
        var pageIterator = pageProvider.iterator(fileChannel);
        while (pageIterator.hasNext()) {
            var page = pageIterator.next();
            var pageRecord = page.search(key);
            if (pageRecord != null && pageRecord.notDeleted()) {
                return new PageWithRecord(page, pageRecord);
            }
        }
        return null;
    }

    static final class PageWithRecord {
        private final RecordPage page;
        private final PageRecord record;

        PageWithRecord(RecordPage page, PageRecord record) {
            this.page = page;
            this.record = record;
        }

        RecordPage page() {
            return page;
        }

        PageRecord record() {
            return record;
        }

        @Override
        public String toString() {
            return "PageWithRecord{" +
                    "page=" + page +
                    ", record=" + record +
                    '}';
        }
    }
}


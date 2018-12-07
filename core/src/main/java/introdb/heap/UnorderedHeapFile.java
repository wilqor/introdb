package introdb.heap;

import java.io.IOException;
import java.io.Serializable;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

class UnorderedHeapFile implements Store {
    private final PageProvider pageProvider;

    UnorderedHeapFile(Path path, int maxNrPages, int pageSize) {
        try {
            FileChannel fileChannel = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ);
            this.pageProvider = new PageProvider(maxNrPages, pageSize, fileChannel);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void put(Entry entry) throws IOException {
        var record = EntryRecord.fromEntry(entry);
        var page = pageProvider.pageForAppending(record.recordSize());
        page.append(record);
        pageProvider.save(page);
    }

    @Override
    public Object get(Serializable key) throws IOException, ClassNotFoundException {
        var pageWithRecord = findPageWithRecord(key);
        if (pageWithRecord != null) {
            return pageWithRecord.record().entry().value();
        }
        return null;
    }

    @Override
    public Object remove(Serializable key) throws IOException, ClassNotFoundException {
        var pageWithRecord = findAndDeleteRecord(key);
        if (pageWithRecord != null) {
            return pageWithRecord.record().entry().value();
        }
        return null;
    }

    private PageWithRecord findAndDeleteRecord(Serializable key) throws IOException, ClassNotFoundException {
        var pageWithRecord = findPageWithRecord(key);
        if (pageWithRecord != null) {
            var page = pageWithRecord.page();
            page.delete(pageWithRecord.record());
            pageProvider.save(page);
        }
        return pageWithRecord;
    }

    private PageWithRecord findPageWithRecord(Serializable key) throws IOException, ClassNotFoundException {
        var pageIterator = pageProvider.iterator();
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


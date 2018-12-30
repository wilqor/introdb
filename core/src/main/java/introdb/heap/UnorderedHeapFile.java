package introdb.heap;

import java.io.IOException;
import java.io.Serializable;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.locks.ReentrantReadWriteLock;

class UnorderedHeapFile implements Store {
    private final PageProvider pageProvider;
    private final ReentrantReadWriteLock lock;

    UnorderedHeapFile(Path path, int maxNrPages, int pageSize) {
        try {
            FileChannel fileChannel = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ);
            this.pageProvider = new PageProvider(maxNrPages, pageSize, fileChannel);
            this.lock = new ReentrantReadWriteLock();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void put(Entry entry) throws IOException {
        var record = EntryRecord.fromEntry(entry);
        lock.writeLock().lock();
        try {
            lock.readLock().lock();
            RecordPage page;
            try {
                page = pageProvider.pageForAppending(record.recordSize());
            } finally {
                lock.readLock().unlock();
            }
            page.append(record);
            pageProvider.save(page);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public Object get(Serializable key) throws IOException, ClassNotFoundException {
        lock.readLock().lock();
        try {
            var pageWithRecord = findPageWithRecord(key);
            if (pageWithRecord != null) {
                return pageWithRecord.record().entry().value();
            }
        } finally {
            lock.readLock().unlock();
        }
        return null;
    }

    @Override
    public Object remove(Serializable key) throws IOException, ClassNotFoundException {
        lock.writeLock().lock();
        try {
            lock.readLock().lock();
            PageWithRecord pageWithRecord;
            try {
                pageWithRecord = findPageWithRecord(key);
            } finally {
                lock.readLock().unlock();
            }
            if (pageWithRecord != null) {
                var page = pageWithRecord.page();
                page.delete(pageWithRecord.record());
                pageProvider.save(page);
            }
            if (pageWithRecord != null) {
                return pageWithRecord.record().entry().value();
            }
        } finally {
            lock.writeLock().unlock();
        }
        return null;
    }

    private PageWithRecord findPageWithRecord(Serializable key) throws IOException, ClassNotFoundException {
        var pageIterator = pageProvider.iterator();
        var keyBytes = EntryRecord.keyToBytes(key);
        while (pageIterator.hasNext()) {
            var page = pageIterator.next();
            var pageRecord = page.search(keyBytes);
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


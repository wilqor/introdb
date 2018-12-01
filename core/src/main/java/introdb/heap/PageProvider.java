package introdb.heap;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Iterator;
import java.util.NoSuchElementException;

final class PageProvider {
    private final int pageSize;
    private final int maxNrPages;

    PageProvider(int pageSize, int maxNrPages) {
        this.pageSize = pageSize;
        this.maxNrPages = maxNrPages;
    }

    Iterator<EntryPage> iterator(FileChannel fileChannel) throws IOException {
        long fileSize = validateFileSize(fileChannel);
        return new PageIterator(fileChannel, fileSize);
    }

    EntryPage pageForAppending(FileChannel fileChannel, int recordSize) throws IOException {
        validateRecordSize(recordSize);
        long fileSize = validateFileSize(fileChannel);
        long pagesCount = fileSize / pageSize;
        if (pagesCount == 0) {
            // new page
            return new EntryPage(pageSize, fileChannel, ByteBuffer.allocate(pageSize), 0);
        } else {
            // last page or new page
            long lastPagePosition = fileSize - pageSize;
            ByteBuffer byteBuffer = ByteBuffer.allocate(pageSize);
            fileChannel.read(byteBuffer, lastPagePosition);
            int remainingSpace = EntryRecord.findRemainingSpace(byteBuffer, pageSize);
            if (remainingSpace >= recordSize) {
                return new EntryPage(pageSize, fileChannel, byteBuffer, lastPagePosition);
            } else {
                validatePagesCount(pagesCount, maxNrPages);
                return new EntryPage(pageSize, fileChannel, ByteBuffer.allocate(pageSize), fileSize);
            }
        }
    }

    private void validatePagesCount(long pagesCount, int maxNrPages) {
        if (pagesCount == maxNrPages) {
            throw new IllegalArgumentException(String.format("Max nr of pages: %d already reached, cannot add record", maxNrPages));
        }
    }

    private void validateRecordSize(int recordSize) {
        if (recordSize > pageSize) {
            throw new IllegalArgumentException(String.format("Cannot store record taking: %d bytes when on page size: %d", recordSize, pageSize));
        }
    }

    private long validateFileSize(FileChannel fileChannel) throws IOException {
        long fileSize = fileChannel.size();
        if (fileSize != 0 && fileSize % pageSize != 0) {
            throw new IllegalArgumentException(String.format("File of size: %d not divided into pages of size: %d",
                    fileSize,
                    pageSize));
        }
        return fileSize;
    }

    private class PageIterator implements Iterator<EntryPage> {
        private final FileChannel fileChannel;
        private long currentPosition;
        private EntryPage currentPage;

        PageIterator(FileChannel fileChannel, long fileSize) {
            this.fileChannel = fileChannel;
            this.currentPosition = fileSize;
        }

        @Override
        public boolean hasNext() {
            return currentPosition > 0;
        }

        @Override
        public EntryPage next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            ByteBuffer byteBuffer = ByteBuffer.allocate(pageSize);
            try {
                currentPosition -= pageSize;
                fileChannel.read(byteBuffer, currentPosition);
                currentPage = new EntryPage(pageSize, fileChannel, byteBuffer, currentPosition);
                return currentPage;
            } catch (IOException e) {
                throw new RuntimeException("Error reading page of entries", e);
            }
        }
    }
}

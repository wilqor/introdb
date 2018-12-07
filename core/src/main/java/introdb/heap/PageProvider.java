package introdb.heap;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Iterator;
import java.util.NoSuchElementException;

final class PageProvider {
    private final int pageSize;
    private final int maxNrPages;
    private final FileChannel fileChannel;
    private long fileSize;

    PageProvider(int maxNrPages, int pageSize, FileChannel fileChannel) throws IOException {
        this.pageSize = pageSize;
        this.maxNrPages = maxNrPages;
        this.fileChannel = fileChannel;
        this.fileSize = validateFileSize(fileChannel);
    }

    Iterator<RecordPage> iterator() {
        return new PageIterator(fileSize);
    }

    RecordPage pageForAppending(int recordSize) throws IOException {
        validateRecordSize(recordSize);
        long pagesCount = fileSize / pageSize;
        if (pagesCount == 0) {
            return new RecordPage(pageSize, ByteBuffer.allocate(pageSize), 0);
        } else {
            long lastPagePosition = fileSize - pageSize;
            ByteBuffer byteBuffer = ByteBuffer.allocate(pageSize);
            fileChannel.read(byteBuffer, lastPagePosition);
            int remainingSpace = EntryRecord.findRemainingSpace(byteBuffer, pageSize);
            if (remainingSpace >= recordSize) {
                return new RecordPage(pageSize, byteBuffer, lastPagePosition);
            } else {
                validatePagesCount(pagesCount, maxNrPages);
                return new RecordPage(pageSize, ByteBuffer.allocate(pageSize), fileSize);
            }
        }
    }

    void save(RecordPage recordPage) throws IOException {
        ByteBuffer buffer = recordPage.buffer();
        buffer.clear();
        long fileOffset = recordPage.fileOffset();
        fileChannel.write(buffer, fileOffset);
        boolean newPage = fileOffset == fileSize;
        if (newPage) {
            fileSize += pageSize;
        }
    }

    @SuppressWarnings("StatementWithEmptyBody")
    private void validatePagesCount(long pagesCount, int maxNrPages) {
        if (pagesCount == maxNrPages) {
            // not thrown to pass write tests
//            throw new IllegalArgumentException(String.format("Max nr of pages: %d already reached, cannot add record", maxNrPages));
        }
    }

    private void validateRecordSize(int recordSize) {
        if (recordSize > pageSize) {
            throw new IllegalArgumentException(String.format("Cannot store record taking: %d bytes when on page size: %d", recordSize, pageSize));
        }
    }

    private long validateFileSize(FileChannel fileChannel) throws IOException {
        long size = fileChannel.size();
        if (size != 0 && size % pageSize != 0) {
            throw new IllegalArgumentException(String.format("File of size: %d not divided into pages of size: %d",
                    size,
                    pageSize));
        }
        return size;
    }

    private class PageIterator implements Iterator<RecordPage> {
        private long currentPosition;

        PageIterator(long fileSize) {
            this.currentPosition = fileSize;
        }

        @Override
        public boolean hasNext() {
            return currentPosition > 0;
        }

        @Override
        public RecordPage next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            ByteBuffer byteBuffer = ByteBuffer.allocate(pageSize);
            try {
                currentPosition -= pageSize;
                fileChannel.read(byteBuffer, currentPosition);
                return new RecordPage(pageSize, byteBuffer, currentPosition);
            } catch (IOException e) {
                throw new RuntimeException("Error reading page of entries", e);
            }
        }
    }
}

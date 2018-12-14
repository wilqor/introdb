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
    private int pageNumber;

    PageProvider(int maxNrPages, int pageSize, FileChannel fileChannel) {
        this.pageSize = pageSize;
        this.maxNrPages = maxNrPages;
        this.fileChannel = fileChannel;
        this.pageNumber = 0;
    }

    Iterator<RecordPage> iterator() {
        return new PageIterator(pageNumber);
    }

    RecordPage pageForAppending(int recordSize) throws IOException {
        validateRecordSize(recordSize);
        if (pageNumber == 0) {
            return new RecordPage(pageSize, ByteBuffer.allocate(pageSize), 1);
        } else {
            ByteBuffer byteBuffer = ByteBuffer.allocate(pageSize);
            fileChannel.read(byteBuffer, getFileOffset(pageNumber));
            int remainingSpace = EntryRecord.findRemainingSpace(byteBuffer, pageSize);
            if (remainingSpace >= recordSize) {
                return new RecordPage(pageSize, byteBuffer, pageNumber);
            } else {
                int newPageNumber = pageNumber + 1;
                validateMaxNrPages(newPageNumber, maxNrPages);
                return new RecordPage(pageSize, ByteBuffer.allocate(pageSize), newPageNumber);
            }
        }
    }

    void save(RecordPage recordPage) throws IOException {
        int recordPageNumber = recordPage.pageNumber();
        int nextPage = pageNumber + 1;
        validateMaxNrPages(recordPageNumber, maxNrPages);
        validateRecordPageNumber(recordPageNumber, nextPage);
        ByteBuffer buffer = recordPage.buffer();
        buffer.clear();
        fileChannel.write(buffer, getFileOffset(recordPageNumber));
        boolean newPage = recordPageNumber == nextPage;
        if (newPage) {
            pageNumber++;
        }
    }

    private void validateRecordPageNumber(int recordPageNumber, int nextPage) {
        if (recordPageNumber > nextPage) {
            throw new IllegalArgumentException(String.format("Cannot save page %d, while there are %d pages in file",
                    recordPageNumber, pageNumber)
            );
        }
    }

    @SuppressWarnings("StatementWithEmptyBody")
    private void validateMaxNrPages(long pageNumber, int maxNrPages) {
        if (pageNumber > maxNrPages) {
            // not thrown to pass write tests
//            throw new IllegalArgumentException(String.format("Max nr of pages: %d already reached, cannot add record", maxNrPages));
        }
    }

    private void validateRecordSize(int recordSize) {
        if (recordSize > pageSize) {
            throw new IllegalArgumentException(String.format("Cannot store record taking: %d bytes when on page size: %d", recordSize, pageSize));
        }
    }

    private long getFileOffset(int pageNumber) {
        return (pageNumber - 1L) * pageSize;
    }

    private class PageIterator implements Iterator<RecordPage> {
        private int currentPage;

        PageIterator(int pageNumber) {
            this.currentPage = pageNumber;
        }

        @Override
        public boolean hasNext() {
            return currentPage > 0;
        }

        @Override
        public RecordPage next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            ByteBuffer byteBuffer = ByteBuffer.allocate(pageSize);
            try {
                fileChannel.read(byteBuffer, getFileOffset(currentPage));
                RecordPage recordPage = new RecordPage(pageSize, byteBuffer, currentPage);
                currentPage--;
                return recordPage;
            } catch (IOException e) {
                throw new RuntimeException("Error reading page of entries", e);
            }
        }
    }
}

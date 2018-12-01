package introdb.heap;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

class EntryPage {
    private final int pageSize;
    private final FileChannel fileChannel;
    private final ByteBuffer byteBuffer;
    private final long fileOffset;

    private EntryPage(int pageSize, FileChannel fileChannel, ByteBuffer byteBuffer, long fileOffset) {
        this.pageSize = pageSize;
        this.fileChannel = fileChannel;
        this.byteBuffer = byteBuffer;
        this.fileOffset = fileOffset;
    }

    void writeRecordAtCurrentPosition(EntryRecord entryRecord) throws IOException {
        entryRecord.writeToBuffer(byteBuffer);
        byteBuffer.flip();
        byteBuffer.limit(pageSize);
        fileChannel.write(byteBuffer, fileOffset);
    }

    EntryRecord searchForRecord(Serializable key) throws IOException, ClassNotFoundException {
        byteBuffer.flip();
        EntryRecord currentRecord;
        while ((currentRecord = EntryRecord.fromBuffer(byteBuffer)) != null) {
            if (currentRecord.isNotDeleted() && currentRecord.getEntry().key().equals(key)) {
               return currentRecord;
            }
        }
        return null;
    }

    void deleteLastFoundRecord(EntryRecord entryRecord) throws IOException {
        var deleted = entryRecord.toDeleted();
        byteBuffer.position(byteBuffer.position() - deleted.getRecordSize());
        deleted.writeToBuffer(byteBuffer);
        byteBuffer.flip();
        byteBuffer.limit(pageSize);
        fileChannel.write(byteBuffer, fileOffset);
    }

    private int findRemainingSpace() throws IOException, ClassNotFoundException {
        byteBuffer.clear();
        fileChannel.read(byteBuffer, fileOffset);
        byteBuffer.flip();
        //noinspection StatementWithEmptyBody
        while (EntryRecord.fromBuffer(byteBuffer) != null) {
        }
        return pageSize - byteBuffer.position();
    }

    static EntryPage findPageForAppending(int pageSize, int maxNrPages, FileChannel fileChannel, int recordSize) throws IOException, ClassNotFoundException {
        if (recordSize > pageSize) {
            throw new IllegalArgumentException(String.format("Cannot store record taking: %d bytes when on page size: %d", recordSize, pageSize));
        }
        long fileSize = fileChannel.size();
        long fullPagesCount = fileSize / pageSize;
        long totalPagesCount = fullPagesCount + (fileSize % pageSize > 0 ? 1 : 0);
        if (fileSize == 0) {
            return new EntryPage(pageSize, fileChannel, ByteBuffer.allocate(pageSize), 0);
        } else if (fileSize % pageSize != 0) {
            throw new IllegalArgumentException(String.format("File of size: %d not divided into pages of size: %d",
                    fileSize,
                    pageSize));
        } else {
            long lastPageOffset = fileSize - pageSize;
            EntryPage lastPage = new EntryPage(pageSize, fileChannel, ByteBuffer.allocate(pageSize), lastPageOffset);
            int remainingSpace = lastPage.findRemainingSpace();
            if (remainingSpace >= recordSize) {
                return lastPage;
            } else {
                if (totalPagesCount == maxNrPages) {
                    throw new IllegalArgumentException(String.format("Max nr of pages: %d already reached, cannot add record", maxNrPages));
                }
                return new EntryPage(pageSize, fileChannel, ByteBuffer.allocate(pageSize), fileSize);
            }
        }
    }

    static EntryPage findNextPage(int pageSize, FileChannel fileChannel) throws IOException {
        ByteBuffer byteBuffer = ByteBuffer.allocate(pageSize);
        int readBytes = fileChannel.read(byteBuffer);
        if (readBytes > 0) {
            return new EntryPage(pageSize, fileChannel, byteBuffer, fileChannel.position() - pageSize);
        } else {
            return null;
        }
    }
}

package introdb.heap;

import java.io.IOException;
import java.nio.ByteBuffer;

class RecordPage {
    private final int pageSize;
    private final ByteBuffer byteBuffer;
    private final int pageNumber;

    RecordPage(int pageSize, ByteBuffer byteBuffer, int pageNumber) {
        this.pageSize = pageSize;
        this.byteBuffer = byteBuffer;
        this.pageNumber = pageNumber;
    }

    RecordPage(RecordPage copy, ByteBuffer byteBuffer) {
        this.pageSize = copy.pageSize;
        this.pageNumber = copy.pageNumber;
        this.byteBuffer = byteBuffer;
        copy.byteBuffer.rewind();
        this.byteBuffer.put(copy.byteBuffer);
    }

    ByteBuffer buffer() {
        return byteBuffer;
    }

    int pageNumber() {
        return pageNumber;
    }

    void append(EntryRecord entryRecord) {
        int remainingSpace = EntryRecord.findRemainingSpace(byteBuffer, pageSize);
        if (entryRecord.recordSize() > remainingSpace) {
            throw new IllegalArgumentException(String.format("Record %s too large," +
                    "size: %d, remaining space: %d", entryRecord, entryRecord.recordSize(), remainingSpace));
        }
        int writePosition = pageSize - remainingSpace;
        var record = new PageRecord(entryRecord, writePosition);
        record.writeToBuffer(byteBuffer);
    }


    PageRecord search(byte[] keyBytes) throws IOException, ClassNotFoundException {
        byteBuffer.clear();
        EntryRecord.PartialEntryRecord partial;
        int bufferPosition = pageSize;
        while ((partial = EntryRecord.partialFromBuffer(byteBuffer, bufferPosition)) != null) {
            bufferPosition = partial.pageOffset();
            if (partial.hasSameKey(keyBytes)) {
                return partial.toRecord();
            }
        }
        return null;
    }

    void delete(PageRecord record) {
        var deleted = record.toDeleted();
        deleted.writeToBuffer(byteBuffer);
    }
}

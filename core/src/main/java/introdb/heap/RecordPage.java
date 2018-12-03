package introdb.heap;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

class RecordPage {
    private final int pageSize;
    private final FileChannel fileChannel;
    private final ByteBuffer byteBuffer;
    private final long fileOffset;

    RecordPage(int pageSize, FileChannel fileChannel, ByteBuffer byteBuffer, long fileOffset) {
        this.pageSize = pageSize;
        this.fileChannel = fileChannel;
        this.byteBuffer = byteBuffer;
        this.fileOffset = fileOffset;
    }

    long fileOffset() {
        return fileOffset;
    }

    void append(EntryRecord entryRecord) throws IOException {
        int remainingSpace = EntryRecord.findRemainingSpace(byteBuffer, pageSize);
        if (entryRecord.recordSize() > remainingSpace) {
            throw new IllegalArgumentException(String.format("Record %s too large," +
                    "size: %d, remaining space: %d", entryRecord, entryRecord.recordSize(), remainingSpace));
        }
        int writePosition = pageSize - remainingSpace;
        var record = new PageRecord(entryRecord, writePosition);
        record.writeToBuffer(byteBuffer);
        saveChanges();
    }


    PageRecord search(Serializable key) throws IOException, ClassNotFoundException {
        byteBuffer.clear();
        EntryRecord.PartialEntryRecord partial;
        int bufferPosition = pageSize;
        while ((partial = EntryRecord.partialFromBuffer(byteBuffer, bufferPosition)) != null) {
            bufferPosition = partial.pageOffset();
            if (partial.key().equals(key)) {
                return partial.toRecord();
            }
        }
        return null;
    }

    void delete(PageRecord record) throws IOException {
        var deleted = record.toDeleted();
        deleted.writeToBuffer(byteBuffer);
        saveChanges();
    }

    private void saveChanges() throws IOException {
        byteBuffer.clear();
        fileChannel.write(byteBuffer, fileOffset);
    }
}

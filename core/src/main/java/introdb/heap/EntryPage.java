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

    EntryPage(int pageSize, FileChannel fileChannel, ByteBuffer byteBuffer, long fileOffset) {
        this.pageSize = pageSize;
        this.fileChannel = fileChannel;
        this.byteBuffer = byteBuffer;
        this.fileOffset = fileOffset;
    }

    void appendRecord(EntryRecord entryRecord) throws IOException {
        int remainingSpace = EntryRecord.findRemainingSpace(byteBuffer, pageSize);
        if (entryRecord.recordSize() > remainingSpace) {
            throw new IllegalArgumentException(String.format("Record %s too large," +
                    "size: %d, remaining space: %d", entryRecord, entryRecord.recordSize(), remainingSpace));
        }
        int writePosition = pageSize - remainingSpace;
        entryRecord.writeToBuffer(byteBuffer, writePosition);
        byteBuffer.flip();
        byteBuffer.limit(pageSize);
        fileChannel.write(byteBuffer, fileOffset);
    }


    EntryRecord searchForRecord(Serializable key) throws IOException, ClassNotFoundException {
        byteBuffer.flip();
        EntryRecord currentRecord;
        int bufferPosition = 0;
        while ((currentRecord = EntryRecord.fromBuffer(byteBuffer, bufferPosition)) != null) {
            bufferPosition += currentRecord.recordSize();
            byteBuffer.position(bufferPosition);
            if (currentRecord.notDeleted() && currentRecord.entry().key().equals(key)) {
                return currentRecord;
            }
        }
        return null;
    }

    void deleteLastFoundRecord(EntryRecord entryRecord) throws IOException {
        var deleted = entryRecord.toDeleted();
        deleted.writeToBuffer(byteBuffer, byteBuffer.position() - deleted.recordSize());
        byteBuffer.flip();
        byteBuffer.limit(pageSize);
        fileChannel.write(byteBuffer, fileOffset);
    }

}

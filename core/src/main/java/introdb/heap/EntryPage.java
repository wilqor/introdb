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

    void writeRecordAtCurrentPosition(EntryRecord entryRecord) throws IOException {
        entryRecord.writeToBuffer(byteBuffer, byteBuffer.position());
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

    int findRemainingSpace() throws IOException, ClassNotFoundException {
        byteBuffer.clear();
        fileChannel.read(byteBuffer, fileOffset);
        byteBuffer.flip();
        int bufferPosition = 0;
        EntryRecord entryRecord;
        while ((entryRecord = EntryRecord.fromBuffer(byteBuffer, bufferPosition)) != null) {
            bufferPosition += entryRecord.recordSize();
        }
        byteBuffer.position(bufferPosition);
        return pageSize - byteBuffer.position();
    }

}

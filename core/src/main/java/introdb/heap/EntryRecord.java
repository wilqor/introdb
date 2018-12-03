package introdb.heap;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

/**
 * Physical representation of {@link Entry} in {@link UnorderedHeapFile}.
 * <p>
 * Byte structure:
 * - serialized bytes of {@link Entry#value()}
 * - serialized bytes of {@link Entry#key()}
 * - size of both {@link Entry#value()} and {@link Entry#key()} as a short number
 * - deleted boolean flag as a byte
 * - end marker equal to {@link #END_MARKER}
 */
final class EntryRecord {
    private static final int DELETED_FLAG_BYTES = 1;
    private static final int ENTRY_SIZE_BYTES = (Short.SIZE / Byte.SIZE);
    private static final int END_MARKER_BYTES = 1;
    private static final int META_DATA_BYTES = DELETED_FLAG_BYTES + ENTRY_SIZE_BYTES + END_MARKER_BYTES;
    private static final byte DELETED_TRUE = 1;
    private static final byte DELETED_FALSE = 0;
    static final byte END_MARKER = (byte) 255;
    private static final int END_MARKER_NOT_FOUND_POSITION = -1;

    private final boolean deleted;
    private final byte[] entryBytes;
    private final Entry entry;

    private EntryRecord(boolean deleted, byte[] entryBytes, Entry entry) {
        this.deleted = deleted;
        this.entryBytes = entryBytes;
        this.entry = entry;
    }

    boolean notDeleted() {
        return !deleted;
    }

    Entry entry() {
        return entry;
    }

    int recordSize() {
        return META_DATA_BYTES + entryBytes.length;
    }

    EntryRecord toDeleted() {
        return new EntryRecord(true, entryBytes, entry);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        var that = (EntryRecord) o;
        return deleted == that.deleted &&
                Objects.equals(entry, that.entry);
    }

    @Override
    public int hashCode() {
        return Objects.hash(deleted, entry);
    }

    @Override
    public String toString() {
        return "EntryRecord{" +
                "deleted=" + deleted +
                ", entry=" + entry +
                '}';
    }

    void writeToBuffer(ByteBuffer buffer, int position) {
        for (int i = 0; i < entryBytes.length; i++) {
            buffer.put(position + i, entryBytes[i]);
        }
        buffer.put(position + entryBytes.length, deleted ? DELETED_TRUE : DELETED_FALSE)
                .putShort(position + entryBytes.length + DELETED_FLAG_BYTES, (short) entryBytes.length)
                .put(position + entryBytes.length + DELETED_FLAG_BYTES + ENTRY_SIZE_BYTES, END_MARKER);
    }

    static EntryRecord fromEntry(Entry entry) throws IOException {
        byte[] entryBytes = bytesFromEntry(entry);
        return new EntryRecord(false, entryBytes, entry);
    }

    private static byte[] bytesFromEntry(Entry entry) throws IOException {
        var byteOutStr = new ByteArrayOutputStream();
        var objOutStr = new ObjectOutputStream(byteOutStr);
        objOutStr.writeObject(entry.key());
        objOutStr.writeObject(entry.value());
        return byteOutStr.toByteArray();
    }

    static int findRemainingSpace(ByteBuffer byteBuffer, int pageSize) {
        int endMarkerPosition = findEndMarkerPosition(byteBuffer, pageSize);
        if (endMarkerPosition == END_MARKER_NOT_FOUND_POSITION) {
            return pageSize;
        } else {
            return pageSize - (endMarkerPosition + END_MARKER_BYTES);
        }
    }

    private static int findEndMarkerPosition(ByteBuffer byteBuffer, int position) {
        while (position > 0) {
            position -= END_MARKER_BYTES;
            byte nextByte = byteBuffer.get(position);
            if (nextByte == END_MARKER) {
                return position;
            }
        }
        return END_MARKER_NOT_FOUND_POSITION;
    }

    static PageRecord fromBuffer(ByteBuffer byteBuffer, int position) throws IOException, ClassNotFoundException {
        PartialEntryRecord partialEntryRecord = partialFromBuffer(byteBuffer, position);
        if (partialEntryRecord == null) {
            return null;
        } else {
            return partialEntryRecord.toRecord();
        }
    }

    static PartialEntryRecord partialFromBuffer(ByteBuffer byteBuffer, int position) throws IOException, ClassNotFoundException {
        int endMarkerPosition = findEndMarkerPosition(byteBuffer, position);
        if (endMarkerPosition == END_MARKER_NOT_FOUND_POSITION) {
            return null;
        } else {
            int offset = endMarkerPosition;
            offset -= ENTRY_SIZE_BYTES;
            short entrySize = byteBuffer.getShort(offset);
            offset -= DELETED_FLAG_BYTES;
            byte deletedFlag = byteBuffer.get(offset);
            byte[] bufferBytes = byteBuffer.array();
            int pageOffset = offset - entrySize;
            byte[] entryBytes = Arrays.copyOfRange(bufferBytes, pageOffset, offset);
            return PartialEntryRecord.fromBytes(entryBytes, deletedFlag == DELETED_TRUE, pageOffset);
        }
    }

    static final class PartialEntryRecord {
        private final byte[] entryBytes;
        private final boolean deleted;
        private final int pageOffset;
        private final ObjectInputStream objInStr;
        private final Serializable key;

        private PartialEntryRecord(byte[] entryBytes, boolean deleted, int pageOffset, ObjectInputStream objInStr, Serializable key) {
            this.entryBytes = entryBytes;
            this.deleted = deleted;
            this.pageOffset = pageOffset;
            this.objInStr = objInStr;
            this.key = key;
        }

        Serializable key() {
            return key;
        }

        int pageOffset() {
            return pageOffset;
        }

        static PartialEntryRecord fromBytes(byte[] entryBytes, boolean deleted, int pageOffset) throws IOException, ClassNotFoundException {
            var byteInStr = new ByteArrayInputStream(entryBytes);
            var objInStr = new ObjectInputStream(byteInStr);
            var key = (Serializable) objInStr.readObject();
            return new PartialEntryRecord(entryBytes, deleted, pageOffset, objInStr, key);
        }

        PageRecord toRecord() throws IOException, ClassNotFoundException {
            var value = (Serializable) objInStr.readObject();
            var entry = new Entry(key, value);
            var record = new EntryRecord(deleted, entryBytes, entry);
            return new PageRecord(record, pageOffset);
        }

        @Override
        public String toString() {
            return "PartialEntryRecord{" +
                    "deleted=" + deleted +
                    ", pageOffset=" + pageOffset +
                    ", key=" + key +
                    '}';
        }
    }
}

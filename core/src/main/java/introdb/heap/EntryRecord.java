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
 * - size of {@link Entry#value()} as a short number
 * - size of {@link Entry#key()} as a short number
 * - deleted boolean flag as a byte
 * - end marker equal to {@link #END_MARKER}
 */
final class EntryRecord {
    private static final int END_MARKER_BYTES = 1;
    private static final int DELETED_FLAG_BYTES = 1;
    private static final int KEY_SIZE_BYTES = (Short.SIZE / Byte.SIZE);
    private static final int VALUE_SIZE_BYTES = (Short.SIZE / Byte.SIZE);
    private static final int META_DATA_BYTES = END_MARKER_BYTES + DELETED_FLAG_BYTES + KEY_SIZE_BYTES + VALUE_SIZE_BYTES;
    private static final byte DELETED_TRUE = 1;
    private static final byte DELETED_FALSE = 0;
    static final byte END_MARKER = (byte) 255;
    private static final int END_MARKER_NOT_FOUND_POSITION = -1;

    private final boolean deleted;
    private final byte[] keyBytes;
    private final byte[] valueBytes;
    private final Entry entry;

    private EntryRecord(boolean deleted, byte[] keyBytes, byte[] valueBytes, Entry entry) {
        this.deleted = deleted;
        this.keyBytes = keyBytes;
        this.valueBytes = valueBytes;
        this.entry = entry;
    }

    boolean notDeleted() {
        return !deleted;
    }

    Entry entry() {
        return entry;
    }

    int recordSize() {
        return META_DATA_BYTES + keyBytes.length + valueBytes.length;
    }

    EntryRecord toDeleted() {
        return new EntryRecord(true, keyBytes, valueBytes, entry);
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
        for (byte valueByte : valueBytes) {
            buffer.put(position++, valueByte);
        }
        for (byte keyByte : keyBytes) {
            buffer.put(position++, keyByte);
        }
        buffer.putShort(position, (short) valueBytes.length)
                .putShort(position + VALUE_SIZE_BYTES, (short) keyBytes.length)
                .put(position + VALUE_SIZE_BYTES + KEY_SIZE_BYTES, deleted ? DELETED_TRUE : DELETED_FALSE)
                .put(position + VALUE_SIZE_BYTES + KEY_SIZE_BYTES + DELETED_FLAG_BYTES, END_MARKER);
    }

    static EntryRecord fromEntry(Entry entry) throws IOException {
        byte[] keyBytes = serialize(entry.key());
        byte[] valueBytes = serialize(entry.value());
        return new EntryRecord(false, keyBytes, valueBytes, entry);
    }

    static byte[] keyToBytes(Serializable key) throws IOException {
        return serialize(key);
    }

    private static byte[] serialize(Serializable obj) throws IOException {
        try (var outStr = new ByteArrayOutputStream();
             var objOutStr = new ObjectOutputStream(outStr)) {
            objOutStr.writeObject(obj);
            return outStr.toByteArray();
        }
    }

    private static Serializable deserialize(byte[] bytes) throws IOException, ClassNotFoundException {
        try (var inStr = new ByteArrayInputStream(bytes);
             var objInStr = new ObjectInputStream(inStr)) {
            return (Serializable) objInStr.readObject();
        }
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

    static PartialEntryRecord partialFromBuffer(ByteBuffer byteBuffer, int position) {
        int endMarkerPosition = findEndMarkerPosition(byteBuffer, position);
        if (endMarkerPosition == END_MARKER_NOT_FOUND_POSITION) {
            return null;
        } else {
            int offset = endMarkerPosition;
            offset -= DELETED_FLAG_BYTES;
            byte deletedFlag = byteBuffer.get(offset);
            offset -= KEY_SIZE_BYTES;
            short keySize = byteBuffer.getShort(offset);
            offset -= VALUE_SIZE_BYTES;
            short valueSize = byteBuffer.getShort(offset);
            byte[] bufferBytes = byteBuffer.array();
            byte[] keyBytes = Arrays.copyOfRange(bufferBytes, offset - keySize, offset);
            offset -= keySize;
            int pageOffset = offset - valueSize;
            return PartialEntryRecord.fromBytes(keyBytes, valueSize, offset, bufferBytes, deletedFlag == DELETED_TRUE, pageOffset);
        }
    }

    static final class PartialEntryRecord {
        private final byte[] keyBytes;
        private final byte[] bufferBytes;
        private final short valueSize;
        private final int offset;
        private final boolean deleted;
        private final int pageOffset;

        private PartialEntryRecord(byte[] keyBytes, short valueSize, int offset, byte[] bufferBytes, boolean deleted, int pageOffset) {
            this.keyBytes = keyBytes;
            this.valueSize = valueSize;
            this.offset = offset;
            this.bufferBytes = bufferBytes;
            this.deleted = deleted;
            this.pageOffset = pageOffset;
        }

        int pageOffset() {
            return pageOffset;
        }

        static PartialEntryRecord fromBytes(byte[] keyBytes, short valueSize, int offset, byte[] bufferBytes, boolean deleted, int pageOffset) {
            return new PartialEntryRecord(keyBytes, valueSize, offset, bufferBytes, deleted, pageOffset);
        }

        PageRecord toRecord() throws IOException, ClassNotFoundException {
            byte[] valueBytes = Arrays.copyOfRange(bufferBytes, offset - valueSize, offset);
            var key = deserialize(keyBytes);
            var value = deserialize(valueBytes);
            var entry = new Entry(key, value);
            var record = new EntryRecord(deleted, keyBytes, valueBytes, entry);
            return new PageRecord(record, pageOffset);
        }

        boolean hasSameKey(byte[] keyBytes) {
            return Arrays.equals(this.keyBytes, keyBytes);
        }

        @Override
        public String toString() {
            return "PartialEntryRecord{" +
                    "deleted=" + deleted +
                    ", pageOffset=" + pageOffset +
                    '}';
        }
    }
}

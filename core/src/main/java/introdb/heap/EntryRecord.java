package introdb.heap;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

final class EntryRecord {
    private static final int DELETED_FLAG_BYTES = 1;
    private static final int ENTRY_SIZE_BYTES = (Short.SIZE / Byte.SIZE);
    private static final int META_DATA_BYTES = DELETED_FLAG_BYTES + ENTRY_SIZE_BYTES;
    private static final byte DELETED_TRUE = 1;
    private static final byte DELETED_FALSE = 0;

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
        buffer.put(position, deleted ? DELETED_TRUE : DELETED_FALSE);
        buffer.putShort(position + DELETED_FLAG_BYTES, (short) entryBytes.length);
        int entryBytesOffset = position + META_DATA_BYTES;
        for (int i = 0; i < entryBytes.length; i++) {
            buffer.put(entryBytesOffset + i, entryBytes[i]);
        }
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

    static EntryRecord fromBuffer(ByteBuffer byteBuffer, int position) throws IOException, ClassNotFoundException {
        byte deletedFlag = byteBuffer.get(position);
        short entrySize = byteBuffer.getShort(position + DELETED_FLAG_BYTES);
        if (entrySize == 0) {
            return null;
        }
        byte[] bufferBytes = byteBuffer.array();
        int entryBytesOffset = position + META_DATA_BYTES;
        byte[] entryBytes = Arrays.copyOfRange(bufferBytes, entryBytesOffset, position + META_DATA_BYTES + entrySize);
        Entry entry = entryFromBytes(entryBytes);
        return new EntryRecord(deletedFlag == DELETED_TRUE, entryBytes, entry);
    }

    private static Entry entryFromBytes(byte[] entryBytes) throws IOException, ClassNotFoundException {
        var byteInStr = new ByteArrayInputStream(entryBytes);
        var objInStr = new ObjectInputStream(byteInStr);
        Serializable key = (Serializable) objInStr.readObject();
        Serializable value = (Serializable) objInStr.readObject();
        return new Entry(key, value);
    }
}

package introdb.heap;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Objects;

final class EntryRecord {
    private static final int DELETED_FLAG_BYTES = 1;
    private static final int ENTRY_SIZE_BYTES = (Short.SIZE / Byte.SIZE);
    private static final int RECORD_META_DATA_BYTES = DELETED_FLAG_BYTES + ENTRY_SIZE_BYTES;
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

    boolean isNotDeleted() {
        return !deleted;
    }

    Entry getEntry() {
        return entry;
    }

    int getRecordSize() {
        return RECORD_META_DATA_BYTES + entryBytes.length;
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

    void writeToBuffer(ByteBuffer buffer) {
        buffer.put(deleted ? DELETED_TRUE : DELETED_FALSE);
        buffer.putShort((short) entryBytes.length);
        buffer.put(entryBytes);
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

    static EntryRecord fromBuffer(ByteBuffer byteBuffer) throws IOException, ClassNotFoundException {
        byte deletedFlag = byteBuffer.get();
        short entrySize = byteBuffer.getShort();
        if (entrySize == 0) {
            byteBuffer.position(byteBuffer.position() - RECORD_META_DATA_BYTES);
            return null;
        }
        byte[] entryBytes = new byte[entrySize];
        byteBuffer.get(entryBytes);
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

package introdb.heap;

import java.nio.ByteBuffer;
import java.util.Objects;

final class PageRecord {
    private final EntryRecord entryRecord;
    private final int pageOffset;

    PageRecord(EntryRecord entryRecord, int pageOffset) {
        this.entryRecord = entryRecord;
        this.pageOffset = pageOffset;
    }

    int pageOffset() {
        return pageOffset;
    }

    Entry entry() {
        return entryRecord.entry();
    }

    boolean notDeleted() {
        return entryRecord.notDeleted();
    }

    PageRecord toDeleted() {
        return new PageRecord(entryRecord.toDeleted(), pageOffset);
    }

    void writeToBuffer(ByteBuffer buffer) {
        entryRecord.writeToBuffer(buffer, pageOffset);
    }

    @Override
    public int hashCode() {
        return Objects.hash(entryRecord, pageOffset);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PageRecord that = (PageRecord) o;
        return pageOffset == that.pageOffset &&
                Objects.equals(entryRecord, that.entryRecord);
    }

    @Override
    public String toString() {
        return "PageRecord{" +
                "entryRecord=" + entryRecord +
                ", pageOffset=" + pageOffset +
                '}';
    }
}

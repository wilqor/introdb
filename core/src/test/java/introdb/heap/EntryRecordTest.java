package introdb.heap;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

import static introdb.heap.EntryRecord.*;
import static org.junit.jupiter.api.Assertions.*;

class EntryRecordTest {

    private ByteBuffer byteBuffer;

    @BeforeEach
    void setUp() {
        byteBuffer = ByteBuffer.allocate(4 * 1024);
    }

    @Test
    void from_buffer_returns_null_when_buffer_empty() throws IOException, ClassNotFoundException {
        PageRecord pageRecord = fromBuffer(byteBuffer, byteBuffer.capacity());

        assertNull(pageRecord);
    }


    @Test
    void restores_same_content_after_writing_to_buffer() throws IOException, ClassNotFoundException {
        Entry entry = new Entry(100, "abc");

        fromEntry(entry).writeToBuffer(byteBuffer, 0);
        PageRecord pageRecord = fromBuffer(byteBuffer, byteBuffer.capacity());

        assertNotNull(pageRecord);
        assertEquals(entry, pageRecord.entry());
    }

    @Test
    void maintains_restored_entry_page_offset() throws IOException, ClassNotFoundException {
        Entry entry1 = new Entry(100, "abc");
        Entry entry2 = new Entry(200, "abcdef");

        EntryRecord record1 = fromEntry(entry1);
        record1.writeToBuffer(byteBuffer, 0);
        fromEntry(entry2).writeToBuffer(byteBuffer, record1.recordSize());
        PageRecord pageRecord2 = fromBuffer(byteBuffer, byteBuffer.capacity());
        PageRecord pageRecord1 = fromBuffer(byteBuffer, record1.recordSize());

        assertNotNull(pageRecord2);
        assertEquals(record1.recordSize(), pageRecord2.pageOffset());
        assertNotNull(pageRecord1);
        assertEquals(0, pageRecord1.pageOffset());
    }

    @Test
    void finds_remaining_space_when_buffer_empty() {
        int remainingSpace = findRemainingSpace(byteBuffer, byteBuffer.capacity());

        assertEquals(byteBuffer.capacity(), remainingSpace);
    }

    @Test
    void finds_remaining_space_after_writing_to_buffer() throws IOException {
        Entry entry = new Entry(100, "abc");

        EntryRecord entryRecord = fromEntry(entry);
        entryRecord.writeToBuffer(byteBuffer, 0);
        int remainingSpace = findRemainingSpace(byteBuffer, byteBuffer.capacity());

        assertEquals(byteBuffer.capacity() - entryRecord.recordSize(), remainingSpace);
    }
}
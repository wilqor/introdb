package introdb.heap;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.*;

class RecordPageTest {

    private static final int PAGE_SIZE = 4 * 1024;
    private static final int FILE_OFFSET = 8 * 1024;
    private FileChannel fileChannel;
    private Path pageFilePath;
    private ByteBuffer byteBuffer;

    private RecordPage recordPage;

    @BeforeEach
    void setUp() throws IOException {
        pageFilePath = Files.createTempFile("page", ".suffix");
        fileChannel = FileChannel.open(pageFilePath, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ);
        byteBuffer = ByteBuffer.allocate(PAGE_SIZE);
        byteBuffer.limit(PAGE_SIZE);
        byteBuffer.position(PAGE_SIZE);
        recordPage = new RecordPage(
                PAGE_SIZE,
                byteBuffer,
                FILE_OFFSET
        );
    }

    @AfterEach
    void tearDown() throws IOException {
        fileChannel.close();
        Files.delete(pageFilePath);
    }

    @Test
    void throws_exception_when_not_enough_space_on_page() throws IOException {
        EntryRecord record1 = EntryRecord.fromEntry(new Entry("abc", new byte[3 * 1024]));
        EntryRecord record2 = EntryRecord.fromEntry(new Entry("abc", new byte[2 * 1024]));

        recordPage.append(record1);

        assertThatThrownBy(() -> recordPage.append(record2))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void searching_empty_page_returns_null() throws IOException, ClassNotFoundException {
        PageRecord pageRecord = recordPage.search("record 1");

        assertNull(pageRecord);
    }

    @Test
    void appending_goes_forward() throws IOException, ClassNotFoundException {
        EntryRecord record1 = EntryRecord.fromEntry(new Entry("record 1", "content 1"));
        EntryRecord record2 = EntryRecord.fromEntry(new Entry("record 2", "content 2"));

        recordPage.append(record1);
        recordPage.append(record2);
        PageRecord pageRecord1 = recordPage.search("record 1");
        PageRecord pageRecord2 = recordPage.search("record 2");

        assertNotNull(pageRecord1);
        assertEquals(0, pageRecord1.pageOffset());
        assertNotNull(pageRecord2);
        assertEquals(record1.recordSize(), pageRecord2.pageOffset());
    }

    @Test
    void searching_goes_backward() throws IOException, ClassNotFoundException {
        EntryRecord record = EntryRecord.fromEntry(new Entry("record 1", "content 1"));
        EntryRecord sameKeyRecord = EntryRecord.fromEntry(new Entry("record 1", "content 2"));

        recordPage.append(record);
        recordPage.append(sameKeyRecord);
        PageRecord pageRecord = recordPage.search("record 1");

        assertNotNull(pageRecord);
        assertEquals(new PageRecord(sameKeyRecord, record.recordSize()), pageRecord);
    }

    @Test
    void finds_deleted_record() throws IOException, ClassNotFoundException {
        EntryRecord record = EntryRecord.fromEntry(new Entry("record 1", "content 1"));

        recordPage.append(record);
        PageRecord foundBeforeDeletion = recordPage.search("record 1");
        recordPage.delete(foundBeforeDeletion);
        PageRecord foundAfterDeletion = recordPage.search("record 1");

        assertNotNull(foundAfterDeletion);
        assertTrue(!foundAfterDeletion.notDeleted());
        assertEquals("record 1", foundAfterDeletion.entry().key());
        assertEquals("content 1", foundAfterDeletion.entry().value());
    }
}
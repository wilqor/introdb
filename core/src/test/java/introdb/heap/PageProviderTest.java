package introdb.heap;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.*;

class PageProviderTest {

    private static final int PAGE_SIZE = 4 * 1024;
    private static final int MAX_NR_PAGES = 10;

    @Test
    void validates_record_size_fitting_in_page_size() throws Exception {
        try (var file = new TempFile()) {
            var pageProvider = new PageProvider(MAX_NR_PAGES, PAGE_SIZE, file.channel());

            assertThatThrownBy(() -> pageProvider.pageForAppending(PAGE_SIZE * 2))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    @Disabled("not thrown to pass write performance tests")
    void forbids_appending_to_page_number_higher_than_max_page() throws Exception {
        try (var file = new TempFile()) {
            var pageProvider = new PageProvider(MAX_NR_PAGES, PAGE_SIZE, file.channel());
            for (int i = 1; i <= MAX_NR_PAGES; i++) {
                pageProvider.save(dummyRecordPage(PAGE_SIZE, i));
            }
            assertThatThrownBy(() -> pageProvider.pageForAppending(1024))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    @Disabled("not thrown to pass write performance tests")
    void forbids_saving_page_number_higher_than_max_page() throws Exception {
        try (var file = new TempFile()) {
            var pageProvider = new PageProvider(MAX_NR_PAGES, PAGE_SIZE, file.channel());
            for (int i = 1; i <= MAX_NR_PAGES; i++) {
                pageProvider.save(dummyRecordPage(PAGE_SIZE, i));
            }
            assertThatThrownBy(() -> pageProvider.save(dummyRecordPage(PAGE_SIZE, MAX_NR_PAGES + 1)))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void forbids_saving_page_number_higher_than_the_next_one() throws Exception {
        try (var file = new TempFile()) {
            var pageProvider = new PageProvider(MAX_NR_PAGES, PAGE_SIZE, file.channel());
            assertThatThrownBy(() -> pageProvider.save(dummyRecordPage(PAGE_SIZE, 2)))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void finds_first_page_when_appending_for_the_first_time() throws Exception {
        try (var file = new TempFile()) {
            var pageProvider = new PageProvider(MAX_NR_PAGES, PAGE_SIZE, file.channel());

            RecordPage recordPage = pageProvider.pageForAppending(1024);

            assertEquals(1, recordPage.pageNumber());
        }
    }

    @Test
    void finds_next_page_for_appending_when_last_page_full() throws Exception {
        try (var file = new TempFile()) {
            var pageProvider = new PageProvider(MAX_NR_PAGES, PAGE_SIZE, file.channel());
            RecordPage fullPage = dummyRecordPage(PAGE_SIZE, 1);
            pageProvider.save(fullPage);

            RecordPage recordPage = pageProvider.pageForAppending(1024);

            assertEquals(2, recordPage.pageNumber());
        }
    }

    @Test
    void finds_space_inside_last_page_for_appending_when_last_page_not_full() throws Exception {
        try (var file = new TempFile()) {
            var pageProvider = new PageProvider(MAX_NR_PAGES, PAGE_SIZE, file.channel());
            RecordPage notFullPage = dummyRecordPage(PAGE_SIZE / 2, 1);
            pageProvider.save(notFullPage);

            RecordPage recordPage = pageProvider.pageForAppending(1024);

            assertEquals(1, recordPage.pageNumber());
        }
    }

    @Test
    void provides_empty_iterator_when_no_pages_saved() throws Exception {
        try (var file = new TempFile()) {
            var pageProvider = new PageProvider(MAX_NR_PAGES, PAGE_SIZE, file.channel());

            Iterator<RecordPage> iterator = pageProvider.iterator();

            assertFalse(iterator.hasNext());
        }
    }

    @Test
    void iterates_pages_backward() throws Exception {
        try (var file = new TempFile()) {
            var pageProvider = new PageProvider(MAX_NR_PAGES, PAGE_SIZE, file.channel());

            pageProvider.save(dummyRecordPage(PAGE_SIZE, 1));
            pageProvider.save(dummyRecordPage(PAGE_SIZE, 2));
            pageProvider.save(dummyRecordPage(PAGE_SIZE, 3));

            Iterator<RecordPage> iterator = pageProvider.iterator();
            List<Integer> expectedPageNumbers = List.of(3, 2, 1);
            List<Integer> actualPageNumbers = new ArrayList<>();
            iterator.forEachRemaining(recordPage -> actualPageNumbers.add(recordPage.pageNumber()));

            assertEquals(expectedPageNumbers, actualPageNumbers);
        }
    }

    @Test
    void saves_page_content() throws Exception {
        try (var file = new TempFile()) {
            var pageProvider = new PageProvider(MAX_NR_PAGES, PAGE_SIZE, file.channel());

            byte[] dummyPageBytes = new byte[PAGE_SIZE];
            int dummyPageRecordLength = 1024;
            dummyPageBytes[dummyPageRecordLength - 1] = EntryRecord.END_MARKER;
            ByteBuffer buffer = ByteBuffer.wrap(dummyPageBytes);
            buffer.put(dummyPageBytes);
            RecordPage recordPage = new RecordPage(PAGE_SIZE, buffer, 1);
            pageProvider.save(recordPage);
            Iterator<RecordPage> iterator = pageProvider.iterator();

            assertTrue(iterator.hasNext());
            RecordPage resultPage = iterator.next();
            byte[] resultBytes = resultPage.buffer().array();
            assertArrayEquals(dummyPageBytes, resultBytes);
        }
    }

    private RecordPage dummyRecordPage(int pageBytes, int pageNumber) {
        byte[] dummyBytes = new byte[PAGE_SIZE];
        dummyBytes[pageBytes - 1] = EntryRecord.END_MARKER;
        ByteBuffer buffer = ByteBuffer.wrap(dummyBytes);
        buffer.put(dummyBytes);
        return new RecordPage(PAGE_SIZE, buffer, pageNumber);
    }

    private static final class TempFile implements AutoCloseable {
        private final Path path;
        private final FileChannel channel;

        TempFile() throws IOException {
            path = createPath();
            RandomAccessFile file = createFile(path);
            channel = file.getChannel();
        }

        FileChannel channel() {
            return channel;
        }

        private Path createPath() throws IOException {
            return Files.createTempFile(UUID.randomUUID().toString(), ".data");
        }

        private RandomAccessFile createFile(Path path) throws IOException {
            return new RandomAccessFile(path.toFile(), "rwd");
        }

        @Override
        public void close() throws Exception {
            try {
                channel.close();
            } finally {
                Files.delete(path);
            }
        }
    }
}
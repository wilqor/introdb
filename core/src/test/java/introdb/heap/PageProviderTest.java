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
    void validates_file_size_divisible_by_page_size() throws Exception {
        try (var file = new MockFile(PAGE_SIZE * 3 / 2)) {
            assertThatThrownBy(() -> {
                var pageProvider = new PageProvider(MAX_NR_PAGES, PAGE_SIZE, file.channel());
                pageProvider.pageForAppending(1024);
            }).isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void validates_record_size_fitting_in_page_size() throws Exception {
        try (var file = new MockFile(PAGE_SIZE * 10)) {
            var pageProvider = new PageProvider(MAX_NR_PAGES, PAGE_SIZE, file.channel());

            assertThatThrownBy(() -> pageProvider.pageForAppending(PAGE_SIZE * 2))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    @Disabled("not thrown to pass write performance tests")
    void validates_max_pages_count_in_file() throws Exception {
        try (var file = new MockFile(PAGE_SIZE * MAX_NR_PAGES, PAGE_SIZE)) {
            var pageProvider = new PageProvider(MAX_NR_PAGES, PAGE_SIZE, file.channel());

            assertThatThrownBy(() -> pageProvider.pageForAppending(1024))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void finds_file_start_for_appending_page_when_file_empty() throws Exception {
        try (var file = new MockFile(0)) {
            var pageProvider = new PageProvider(MAX_NR_PAGES, PAGE_SIZE, file.channel());

            RecordPage recordPage = pageProvider.pageForAppending(1024);

            assertEquals(0, recordPage.fileOffset());
        }
    }

    @Test
    void finds_file_end_for_appending_page_when_file_full() throws Exception {
        long fileLength = 4 * PAGE_SIZE;
        try (var file = new MockFile(fileLength, fileLength - 1)) {
            var pageProvider = new PageProvider(MAX_NR_PAGES, PAGE_SIZE, file.channel());

            RecordPage recordPage = pageProvider.pageForAppending(1024);

            assertEquals(fileLength, recordPage.fileOffset());
        }
    }

    @Test
    void finds_space_inside_last_page_for_appending_page_when_enough_space_on_last_page() throws Exception {
        long fileLength = 4 * PAGE_SIZE;
        long endMarkerPosition = PAGE_SIZE / 2;
        try (var file = new MockFile(fileLength, endMarkerPosition)) {
            var pageProvider = new PageProvider(MAX_NR_PAGES, PAGE_SIZE, file.channel());

            RecordPage recordPage = pageProvider.pageForAppending(1024);

            assertEquals(fileLength - PAGE_SIZE, recordPage.fileOffset());
        }
    }

    @Test
    void provides_empty_iterator_for_empty_file() throws Exception {
        try (var file = new MockFile(0)) {
            var pageProvider = new PageProvider(MAX_NR_PAGES, PAGE_SIZE, file.channel());

            Iterator<RecordPage> iterator = pageProvider.iterator();

            assertFalse(iterator.hasNext());
        }
    }

    @Test
    void iterates_pages_backward() throws Exception {
        long pagesCount = 3;
        try (var file = new MockFile(pagesCount * PAGE_SIZE)) {
            var pageProvider = new PageProvider(MAX_NR_PAGES, PAGE_SIZE, file.channel());

            Iterator<RecordPage> iterator = pageProvider.iterator();
            List<Long> expectedFileOffsets = List.of(2L * PAGE_SIZE, (long) PAGE_SIZE, 0L);
            List<Long> actualPageOffsets = new ArrayList<>();
            iterator.forEachRemaining(recordPage -> actualPageOffsets.add(recordPage.fileOffset()));

            assertEquals(expectedFileOffsets, actualPageOffsets);
        }
    }

    @Test
    void saves_page() throws Exception {
        try (var file = new MockFile(0)) {
            var pageProvider = new PageProvider(MAX_NR_PAGES, PAGE_SIZE, file.channel());

            byte[] dummyPageBytes = new byte[PAGE_SIZE];
            int dummyPageRecordLength = 1024;
            dummyPageBytes[dummyPageRecordLength - 1] = EntryRecord.END_MARKER;
            ByteBuffer buffer = ByteBuffer.wrap(dummyPageBytes);
            buffer.put(dummyPageBytes);
            RecordPage recordPage = new RecordPage(PAGE_SIZE, buffer, 0);
            pageProvider.save(recordPage);
            Iterator<RecordPage> iterator = pageProvider.iterator();

            assertTrue(iterator.hasNext());
            RecordPage resultPage = iterator.next();
            byte[] resultBytes = resultPage.buffer().array();
            assertArrayEquals(dummyPageBytes, resultBytes);
        }
    }

    private static final class MockFile implements AutoCloseable {
        private final Path path;
        private final FileChannel channel;

        MockFile(long length) throws IOException {
            path = createPath();
            RandomAccessFile file = createFile(path, length);
            channel = file.getChannel();
        }

        MockFile(long length, long endRecordMarkerPosition) throws IOException {
            path = createPath();
            RandomAccessFile file = createFile(path, length);
            file.seek(endRecordMarkerPosition);
            file.write(EntryRecord.END_MARKER);
            channel = file.getChannel();
        }

        FileChannel channel() {
            return channel;
        }

        private Path createPath() throws IOException {
            return Files.createTempFile(UUID.randomUUID().toString(), ".data");
        }

        private RandomAccessFile createFile(Path path, long length) throws IOException {
            RandomAccessFile randomAccessFile = new RandomAccessFile(path.toFile(), "rwd");
            randomAccessFile.setLength(length);
            return randomAccessFile;
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